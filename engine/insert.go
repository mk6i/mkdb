package engine

import (
	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

func EvaluateInsert(q sql.InsertStatement, db string) (int, error) {
	path, err := DBPath(db)
	if err != nil {
		return 0, err
	}

	tbl := q.TableName
	cols := q.InsertColumnsAndSource.InsertColumnList.ColumnNames
	vals := q.InsertColumnsAndSource.QueryExpression.(sql.TableValueConstructor).TableValueConstructorList

	count := 0
	for _, tvc := range vals {
		if err := storage.Insert(path, tbl, cols, tvc.RowValueConstructorList); err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}
