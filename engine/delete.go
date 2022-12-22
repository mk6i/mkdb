package engine

import (
	"github.com/mkaminski/bkdb/sql"
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
	for _, row := range rows {
		if err := rm.MarkDeleted(q.TableName, row.RowID); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}
