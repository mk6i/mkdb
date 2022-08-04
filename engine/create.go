package engine

import (
	"errors"
	"os"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

var (
	errDBExists = errors.New("database already exists")
)

func EvaluateCreateDatabase(q sql.CreateDatabase) error {
	path, err := DBPath(q.Name)
	if err == errDBExists {
		return errDBExists
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	defer file.Close()

	return btree.CreateDB(path)
}

func EvaluateCreateTable(q sql.CreateTable, db string) error {
	r := &btree.Relation{}

	for _, elem := range q.Elements {
		fd := btree.FieldDef{
			Name: elem.ColumnDefinition.Name,
		}
		switch t := elem.ColumnDefinition.DataType.(type) {
		case sql.NumericType:
			fd.DataType = btree.TYPE_INT
		case sql.CharacterStringType:
			fd.DataType = btree.TYPE_VARCHAR
			fd.Len = int32(t.Len)
		default:
			panic("unsupported column definition type")
		}
		r.Fields = append(r.Fields, fd)
	}

	path, err := DBPath(db)
	if err != nil {
		return err
	}

	return btree.CreateTable(path, r, q.Name)
}
