package engine

import (
	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateUpdate(q sql.UpdateStatementSearched, db string, fetcher btree.Fetcher) error {

	path, err := DBPath(db)
	if err != nil {
		return err
	}

	table := q.TableName
	rows, fields, err := fetcher(path, string(table))
	if err != nil {
		return err
	}

	if q.Where != nil {
		rows, err = filterRows(q.Where.(sql.WhereClause), fields, rows)
		if err != nil {
			return err
		}
	}

	cols := []string{}
	updateSrc := []interface{}{}

	for _, set := range q.Set {
		cols = append(cols, set.ObjectColumn)
		val, err := set.UpdateSource.ColumnName.Val()
		if err != nil {
			return err
		}
		updateSrc = append(updateSrc, val)
	}

	for _, row := range rows {
		if err := btree.Update(path, q.TableName, row.RowID, cols, updateSrc); err != nil {
			return err
		}
	}

	return nil
}
