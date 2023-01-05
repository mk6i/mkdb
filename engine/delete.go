package engine

import (
	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

func EvaluateDelete(q sql.DeleteStatementSearched, rm relationManager) (int, error) {
	table := q.TableName
	rows, fields, err := rm.Fetch(table)
	if err != nil {
		return 0, err
	}

	if q.WhereClause != nil {
		rows, err = filterRows(q.WhereClause.(sql.WhereClause), fields, rows)
		if err != nil {
			return 0, err
		}
	}

	count := 0
	var batch storage.WALBatch

	for _, row := range rows {
		walEntries, err := rm.MarkDeleted(q.TableName, row.RowID)
		if err != nil {
			return 0, err
		}
		batch = append(batch, walEntries...)
		count++
	}

	if err := rm.FlushWALBatch(batch); err != nil {
		return count, err
	}

	return count, nil
}
