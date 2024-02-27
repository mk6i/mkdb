package engine

import (
	"github.com/mk6i/mkdb/sql"
	"github.com/mk6i/mkdb/storage"
)

func EvaluateShowDatabase(q sql.ShowDatabase) ([]*storage.Row, []*storage.Field, error) {
	return storage.ShowDB()
}
