// Package pgsql is a REST Layer resource storage handler for PostgreSQL using pgx
package pgsql

import (
	"context"
	"database/sql"
	"fmt"
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
	return newItemList(raw, q.Window.Offset)
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

	if q.Window.Limit >= 0 {
		str += fmt.Sprintf(" LIMIT %d", q.Window.Limit)
		str += fmt.Sprintf(" OFFSET %d", q.Window.Offset)
	}
	str += ";"
	return str, nil
}

// getDelete returns a SQL DELETE statement that represents the Lookup data
func getDelete(h *Handler, q *query.Query) (string, error) {
	str := "DELETE FROM " + h.tableName + " WHERE "
	qry, err := getQuery(q)
	if err != nil {
		return "", err
	}
	str += qry + ";"
	return str, nil
}

// getInsert returns a SQL INSERT statement constructed from the Item data
func getInsert(h *Handler, i *resource.Item) (string, error) {
	var err error

	a := fmt.Sprintf("INSERT INTO %s(etag,updated,", h.tableName)
	z := fmt.Sprintf("VALUES(%s,%s,", i.ETag, i.Updated.String())
	for k, v := range i.Payload {
		var val string
		a += k + ","
		val, err = valueToString(v)
		if err != nil {
			return "", resource.ErrNotImplemented
		}
		z += val + ","
	}
	// remove trailing commas
	a = a[:len(a)-1] + ")"
	z = z[:len(z)-1] + ")"

	result := fmt.Sprintf("%s %s;", a, z)
	return result, nil
}

// getUpdate returns a SQL INSERT statement constructed from the Item data
func getUpdate(h *Handler, i *resource.Item, o *resource.Item) (string, error) {
	var id, oEtag, iEtag, upd string
	var err error

	id, err = valueToString(o.ID)
	if err != nil {
		return "", resource.ErrNotImplemented
	}
	oEtag, err = valueToString(o.ETag)
	if err != nil {
		return "", resource.ErrNotImplemented
	}
	iEtag, err = valueToString(i.ETag)
	if err != nil {
		return "", resource.ErrNotImplemented
	}
	upd, err = valueToString(i.Updated)
	if err != nil {
		return "", resource.ErrNotImplemented
	}
	a := fmt.Sprintf("UPDATE OR ROLLBACK %s SET etag=%s,updated=%s,", h.tableName, iEtag, upd)
	z := fmt.Sprintf("WHERE id=%s AND etag=%s;", id, oEtag)
	for k, v := range i.Payload {
		if k != "id" {
			var val string
			val, err = valueToString(v)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			a += fmt.Sprintf("%s=%s,", k, val)
		}

	}
	// remove trailing comma
	a = a[:len(a)-1]

	result := fmt.Sprintf("%s %s", a, z)
	return result, nil
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
	// Add the id back (we use the same map hoping the mongoItem won't be stored back)
	id := row["id"]
	etag := row["etag"]
	created := row["created"]
	updated := row["updated"]
	delete(row, "etag")
	delete(row, "updated")

	ct, err := time.Parse("2006-01-02 15:04:05.99999999 -0700 MST", created.(string))
	if err != nil {
		return nil, err
	}
	row["created"] = ct

	tu, err := time.Parse("2006-01-02 15:04:05.99999999 -0700 MST", updated.(string))
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
	var err error
	err = h.session.QueryRow(
		fmt.Sprintf("SELECT etag FROM %s WHERE id='%v'", h.tableName, id)).Scan(&etag)
	if err != nil {
		return err
	}

	// compare the etags to ensure that someone else hasn't scooped us.
	if etag != origEtag {
		return resource.ErrConflict
	}

	return nil
}
