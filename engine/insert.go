package engine

import (
	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateInsert(q sql.InsertStatement, db string) error {
	path, err := DBPath(db)
	if err != nil {
		return err
	}

	tbl := q.TableName
	cols := q.InsertColumnsAndSource.InsertColumnList.ColumnNames
	vals := q.InsertColumnsAndSource.TableValueConstructor.Columns
	return btree.Insert(path, tbl, cols, vals)
}
