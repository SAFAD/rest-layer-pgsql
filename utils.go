package pgsql

import (
	"fmt"
	"strings"
	"time"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
)

// getQuery transform a query into an SQL compliant query
func getQuery(q *query.Query) (string, error) {
	return translatePredicate(q.Predicate)
}

// getSort transform a query.Query into an SQL compliant SORT.
// If the sort list is empty, fallback to id.
func getSort(q *query.Query) string {
	l := q.Sort
	var str string
	if len(l) == 0 {
		return "id"
	}
	for _, sort := range q.Sort {
		if sort.Reversed {
			str += sort.Name + " DESC"
		} else {
			str += sort.Name
		}
		str += ","
	}
	return str[:len(str)-1]
}

func translatePredicate(p query.Predicate) (string, error) {
	var str string
	for _, exp := range p {
		switch t := exp.(type) {
		case query.And:
			var s string
			for _, subExp := range t {
				sb, err := translatePredicate(query.Predicate{subExp})
				if err != nil {
					return "", err
				}
				s += sb + " AND "
			}
			// remove the last " AND "
			str += "(" + s[:len(s)-5] + ")"
		case query.Or:
			var s string
			for _, subExp := range t {
				sb, err := translatePredicate(query.Predicate{subExp})
				if err != nil {
					return "", err
				}
				s += sb + " OR "
			}
			// remove the last " OR "
			str += "(" + s[:len(s)-4] + ")"
		case query.In:
			v, err := valuesToString(t.Values)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " IN (" + v + ")"
		case query.NotIn:
			v, err := valuesToString(t.Values)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " NOT IN (" + v + ")"
		case query.Equal:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			switch t.Value.(type) {
			case string:
				v = strings.Replace(v, "*", "%", -1)
				v = strings.Replace(v, "_", "\\_", -1)
				str += t.Field + " LIKE " + v + " ESCAPE '\\'"
			default:
				str += t.Field + " IS " + v
			}
		case query.NotEqual:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			switch t.Value.(type) {
			case string:
				v = strings.Replace(v, "*", "%", -1)
				v = strings.Replace(v, "_", "\\_", -1)
				str += t.Field + " NOT LIKE " + v + " ESCAPE '\\'"
			default:
				str += t.Field + " IS NOT " + v
			}
		case query.GreaterThan:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " > " + v
		case query.GreaterOrEqual:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " >= " + v
		case query.LowerThan:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " < " + v
		case query.LowerOrEqual:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " <= " + v
		default:
			return "", resource.ErrNotImplemented
		}
	}
	return str, nil
}

// valuesToString combines a list of Values into a single comma separated string
func valuesToString(v []query.Value) (string, error) {
	var str string
	for _, v := range v {
		s, err := valueToString(v)
		if err != nil {
			return "", err
		}
		str += fmt.Sprintf("%s,", s)
	}
	return str[:len(str)-1], nil
}

// valueToString converts a Value into a type-specific string
func valueToString(v query.Value) (string, error) {
	var str string
	var i interface{} = v

	switch i.(type) {
	case int:
		str += fmt.Sprintf("%v", i)
	case float64:
		str += fmt.Sprintf("%v", i)
	case bool:
		str += fmt.Sprintf("%v", i)
	case string:
		str += fmt.Sprintf("'%v'", i)
	case time.Time:
		str += fmt.Sprintf("'%v'", i)
	default:
		str += fmt.Sprintf("'%v'", i)
		//return "", resource.ErrNotImplemented
	}
	return str, nil
}
