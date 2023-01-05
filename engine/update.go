package engine

import (
	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
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

	var batch storage.WALBatch
	for _, row := range rows {
		walEntries, err := rm.Update(q.TableName, row.RowID, cols, updateSrc)
		if err != nil {
			return err
		}
		batch = append(batch, walEntries...)
	}

	if err := rm.FlushWALBatch(batch); err != nil {
		return err
	}

	return nil
}
