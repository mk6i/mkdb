package engine

import (
	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

func EvaluateInsert(q sql.InsertStatement, rm relationManager) (int, error) {
	rm.StartTxn()
	defer rm.EndTxn()

	tbl := q.TableName
	cols := q.InsertColumnsAndSource.InsertColumnList.ColumnNames
	vals := q.InsertColumnsAndSource.QueryExpression.(sql.TableValueConstructor).TableValueConstructorList

	var batch storage.WALBatch

	count := 0
	for _, tvc := range vals {
		walEntries, err := rm.Insert(tbl, cols, tvc.RowValueConstructorList)
		if err != nil {
			return 0, err
		}
		count++
		batch = append(batch, walEntries...)
	}

	if err := rm.FlushWALBatch(batch); err != nil {
		return count, err
	}

	return count, nil
}
