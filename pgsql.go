// Package pgsql is a REST Layer resource storage handler for PostgreSQL using pgx
package pgsql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	//Must be annonymously imported so that we can not use its functions
	_ "github.com/lib/pq"
	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
)

// Handler contains the session and table information for a SQL DB.
type Handler struct {
	session   *sql.DB
	tableName string
}

// NewHandler creates an new SQL DB session handler.
func NewHandler(s *sql.DB, tableName string) *Handler {
	return &Handler{
		session:   s,
		tableName: tableName,
	}
}

// sqlItem is a bson representation of a resource.Item
type sqlItem struct {
	ID      interface{}
	ETag    string
	Updated time.Time
	Payload map[string]interface{}
}

// newsqlItem converts a resource.Item into a sqlItem
func newSQLItem(i *resource.Item) *sqlItem {
	// Filter out id from the payload so we don't store it at all
	p := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			p[k] = v
		}
	}
	return &sqlItem{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: p,
	}
}

// Find is the SELECT query in normal SQL life
func (h Handler) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	var err error
	var rows *sql.Rows                // query result
	var cols []string                 // column names
	raw := []map[string]interface{}{} // holds the raw results as a map of columns:values

	// execute the DB query, get the results
	// build a paginated select statement based
	qry, err := getSelect(h, q)
	if err != nil {
		return nil, err
	}

	// execute the DB query, get the results
	rows, err = h.session.Query(qry)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	cols, err = rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		rowMap := make(map[string]interface{})       // col:val map for a row
		rowVals := make([]interface{}, len(cols))    // values for a row
		rowValPtrs := make([]interface{}, len(cols)) // pointers to row values used by Scan

		// create the pointers to the row value elements
		for i := range cols {
			rowValPtrs[i] = &rowVals[i]
		}

		// scan into the pointer slice (and set the values)
		err := rows.Scan(rowValPtrs...)
		if err != nil {
			return nil, err
		}

		// convert byte arrays to strings
		for i, v := range rowVals {
			b, ok := v.([]byte)
			if ok {
				v = string(b)
			}
			rowMap[cols[i]] = v
		}

		// add the row to the intermediate data structure
		raw = append(raw, rowMap)
	}

	// check for any errors during row iteration
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	// return a *resource.ItemList or an error
	// also check if Window is set at all otherwise use 0 as offset
	var offset int
	if q.Window != nil {
		offset = q.Window.Offset
	}
	return newItemList(raw, offset)
}

// Insert stores new items in the backend store. If any of the items already exist,
// no item should be inserted and a resource.ErrConflict must be returned. The insertion
// of the items is performed automatically.
// TODO: add checking for resource.ErrConflict error.
func (h *Handler) Insert(ctx context.Context, items []*resource.Item) error {
	pItems := make([]*sqlItem, len(items))
	for i, item := range items {
		pItems[i] = newSQLItem(item)
	}

	err := insertItems(h, pItems)
	if err != nil {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Update replaces an item in the backend store with a new version. If the original
// item is not found, a resource.ErrNotFound is returned. If the etags don't match, a
// resource.ErrConflict is returned.
func (h *Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {

	// begin a database transaction
	txPtr, err := h.session.Begin()
	if err != nil {
		return err
	}

	err = compareEtags(h, original.ID, original.ETag)
	if err != nil {
		txPtr.Rollback()
		return err
	}

	s, err := getUpdate(h, item, original)
	if err != nil {
		txPtr.Rollback()
		return err
	}
	_, err = h.session.Exec(s)
	if err != nil {
		txPtr.Rollback()
		return err
	}

	// update succeeded, commit the transaction.
	txPtr.Commit()
	return nil
}

// Delete deletes the provided item by its ID. The Etag of the item stored in the
// backend store must match the Etag of the provided item or a resource.ErrConflict
// must be returned. This check should be performed atomically.
//
// If the provided item were not present in the backend store, a resource.ErrNotFound
// must be returned.
//
// If the removal of the data is not immediate, the method must listen for cancellation
// on the passed ctx. If the operation is stopped due to context cancellation, the
// function must return the result of the ctx.Err() method.
func (h *Handler) Delete(ctx context.Context, item *resource.Item) error {

	// begin a transaction
	txPtr, err := h.session.Begin()
	if err != nil {
		return err
	}

	err = compareEtags(h, item.ID, item.ETag)
	if err != nil {
		txPtr.Rollback()
		return err
	}

	// prepare and execute the delete statement, then finish the transaction
	s := fmt.Sprintf("DELETE FROM %s WHERE id = '%s'", h.tableName, item.ID)
	stmt, err := h.session.Prepare(s)
	if err != nil {
		txPtr.Rollback()
		return err
	}

	_, err = stmt.Exec()
	if err != nil {
		txPtr.Rollback()
		return err
	}

	txPtr.Commit()
	return nil
}

// Clear removes all items matching the lookup and returns the number of items
// removed as the first value.  If a query operation is not implemented
// by the storage handler, a resource.ErrNotImplemented is returned.
func (h Handler) Clear(ctx context.Context, q *query.Query) (int, error) {
	// construct the delete statement from the lookup data
	s, err := getDelete(h, q)
	if err != nil {
		return -1, err // should only be ErrNotImplemented
	}
	result, err := h.session.Exec(s)
	if err != nil {
		return -1, err
	}
	ra, err := result.RowsAffected()
	if err != nil {
		return -1, nil
	}
	return int(ra), nil
}

// insertItems inserts statements in Bulk
func insertItems(h *Handler, items []*sqlItem) error {
	//first we roll over the items
	for _, i := range items {
		var statement bytes.Buffer
		var columns bytes.Buffer
		var rows bytes.Buffer
		// second we create a transaction pointer to make sure all our is in safe environment
		transactionPtr, err := h.session.Begin()
		if err != nil {
			// if the transaction fails, return error
			return err
		}
		//defer closing the connection
		defer h.session.Close()

		// third we prepare the statement
		statement.WriteString("INSERT INTO " + h.tableName)
		columns.WriteString("(etag, ")
		rows.WriteString("VALUES('" + i.ETag + "', ")

		for key, value := range i.Payload {
			//now we prepare the columns and rows
			columns.WriteString(key + ", ")
			v, err := valueToString(value)
			if err != nil {
				return err
			}
			rows.WriteString(v + ", ")
		}
		//now we remove trailing commas
		cString := columns.String()
		cString = cString[:len(cString)-2] + ")"
		var fColumns bytes.Buffer
		fColumns.WriteString(cString)

		rString := rows.String()
		rString = rString[:len(rString)-2] + ") RETURNING id"
		var fRows bytes.Buffer
		fRows.WriteString(rString)

		statement.WriteString(fColumns.String())
		statement.WriteString(fRows.String())

		// all good now commit!
		query := statement.String()
		var ID int
		err = transactionPtr.QueryRow(query).Scan(&ID)
		if err != nil {
			//now we check if the error is because of duplicate key
			errorString := fmt.Sprintln(err)
			if strings.Contains(errorString, "pq: duplicate key value violates unique constraint") {
				// duplicate key error
				transactionPtr.Rollback()
				return resource.ErrConflict
			}
			transactionPtr.Rollback()
			return err
		}
		// all good! commit the query
		err = transactionPtr.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

// getUpdate returns a SQL INSERT statement constructed from the Item data
func getUpdate(h *Handler, i *resource.Item, o *resource.Item) (string, error) {
	var id string
	var err error

	id, err = valueToString(o.ID)
	if err != nil {
		return "", resource.ErrNotImplemented
	}

	a := fmt.Sprintf("UPDATE %s SET etag=%s,", h.tableName, "'"+i.ETag+"'")
	z := fmt.Sprintf("WHERE id=%s AND etag=%s;", id, "'"+o.ETag+"'")
	for k, v := range i.Payload {
		if k != "id" {
			var val string
			val, err = valueToString(v)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			//another cheap hack of the cheapest hacks ever hacked in the history of cheapness
			//but seriously why is time.Time type returns this incompatible format?
			//example: 2018-02-27 23:07:44.4179416 +0100 CET m=+7.679574500
			//the m=+7.679574500 appears from nowhere and is unparsable or formattable
			//TODO: FIXME!
			if k == "updated" {
				val = "'" + time.Now().Format(time.RFC3339) + "'"
			}
			a += fmt.Sprintf("%s=%s,", k, val)
		}

	}
	// remove trailing comma
	a = a[:len(a)-1]

	result := fmt.Sprintf("%s %s", a, z)
	return result, nil
}

// getSelect returns a SQL SELECT statement that represents the Lookup data
func getSelect(h Handler, q *query.Query) (string, error) {
	str := "SELECT * FROM " + h.tableName
	qry, err := getQuery(q)
	if err != nil {
		return "", err
	}
	if qry != "" {
		str += " WHERE " + qry
	}
	if q.Sort != nil {
		str += " ORDER BY " + getSort(q)
	}
	if q.Window != nil && q.Window.Limit >= 0 {
		str += fmt.Sprintf(" LIMIT %d", q.Window.Limit)
		str += fmt.Sprintf(" OFFSET %d", q.Window.Offset)
	}
	str += ";"
	return str, nil
}

// getDelete returns a SQL DELETE statement that represents the Lookup data
func getDelete(h Handler, q *query.Query) (string, error) {
	str := "DELETE FROM " + h.tableName + " WHERE "
	qry, err := getQuery(q)
	if err != nil {
		return "", err
	}
	str += qry + ";"
	return str, nil
}

// newItemList creates a list of resource.Item from a SQL result row slice
func newItemList(rows []map[string]interface{}, offset int) (*resource.ItemList, error) {

	items := make([]*resource.Item, len(rows))
	l := &resource.ItemList{Offset: offset, Total: len(rows), Items: items}
	for i, r := range rows {
		item, err := newItem(r)
		if err != nil {
			return nil, err
		}
		items[i] = item
	}
	return l, nil
}

// newItem creates resource.Item from a SQL result row
func newItem(row map[string]interface{}) (*resource.Item, error) {
	// Add the id back (we use the same map hoping the sqlItem won't be stored back)
	id := row["id"]
	etag := row["etag"]
	delete(row, "etag")
	delete(row, "updated")

	tu, err := time.Parse(time.RFC3339, time.Now().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	return &resource.Item{
		ID:      id,
		ETag:    etag.(string),
		Updated: tu,
		Payload: row,
	}, nil
}

func compareEtags(h *Handler, id, origEtag interface{}) error {
	// query for record with the same id, and return ErrNotFound if we don't find one.
	var etag string
	//create a pointer
	transactionPtr, err := h.session.Begin()
	if err != nil {
		return err
	}

	transactionPtr.QueryRow("SELECT etag FROM $1 WHERE id='$2'", h.tableName, id).Scan(&etag)

	err = transactionPtr.Commit()
	if err != nil {
		transactionPtr.Rollback()
		return err
	}

	// compare the etags to ensure that someone else hasn't scooped us.
	if etag != origEtag {
		return resource.ErrConflict
	}

	return nil
}
