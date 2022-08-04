package engine

import (
	"strings"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateInsert(q sql.InsertStatement, db string) error {
	path := "data/" + strings.ToLower(db)
	tbl := q.TableName
	cols := q.InsertColumnsAndSource.InsertColumnList.ColumnNames
	vals := q.InsertColumnsAndSource.TableValueConstructor.Columns
	return btree.Insert(path, tbl, cols, vals)
}
