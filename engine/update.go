package engine

import (
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateUpdate(q sql.UpdateStatementSearched, rm relationManager) error {
	table := q.TableName
	rows, fields, err := rm.Fetch(table)
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
		if err := rm.Update(q.TableName, row.RowID, cols, updateSrc); err != nil {
			return err
		}
	}

	return nil
}
