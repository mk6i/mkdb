package engine

import (
	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateDelete(q sql.DeleteStatementSearched, path string, fetcher btree.Fetcher, deleter btree.Deleter) (int, error) {
	table := q.TableName
	rows, fields, err := fetcher(path, table)
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
		if err := deleter(path, q.TableName, row.RowID); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}
