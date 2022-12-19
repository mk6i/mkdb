package engine

import (
	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

func EvaluateUpdate(q sql.UpdateStatementSearched, db string, fetch Fetcher) error {

	path, err := DBPath(db)
	if err != nil {
		return err
	}

	table := q.TableName
	rows, fields, err := fetch(path, table)
	if err != nil {
		return err
	}

	if q.Where != nil {
		rows, err = filterRows(q.Where.(sql.WhereClause), fields, rows)
		if err != nil {
			return err
		}
	}

	var cols []string
	var updateSrc []interface{}

	for _, set := range q.Set {
		cols = append(cols, set.ObjectColumn)
		val, err := set.UpdateSource.ColumnName.Val()
		if err != nil {
			return err
		}
		updateSrc = append(updateSrc, val)
	}

	for _, row := range rows {
		if err := storage.Update(path, q.TableName, row.RowID, cols, updateSrc); err != nil {
			return err
		}
	}

	return nil
}
