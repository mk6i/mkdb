package engine

import (
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateInsert(q sql.InsertStatement, rm relationManager) (int, error) {
	tbl := q.TableName
	cols := q.InsertColumnsAndSource.InsertColumnList.ColumnNames
	vals := q.InsertColumnsAndSource.QueryExpression.(sql.TableValueConstructor).TableValueConstructorList

	count := 0
	for _, tvc := range vals {
		if err := rm.Insert(tbl, cols, tvc.RowValueConstructorList); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}
