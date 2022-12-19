package engine

import (
	"os"

	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

func EvaluateCreateDatabase(q sql.CreateDatabase) error {
	path, err := DBPath(q.Name)
	if err == nil {
		return errDBExists
	}
	if err != errDBNotExist {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	defer file.Close()

	return storage.CreateDB(path)
}

func EvaluateCreateTable(q sql.CreateTable, db string) error {
	r := &storage.Relation{}

	for _, elem := range q.Elements {
		fd := storage.FieldDef{
			Name: elem.ColumnDefinition.Name,
		}
		switch t := elem.ColumnDefinition.DataType.(type) {
		case sql.NumericType:
			fd.DataType = storage.TypeInt
		case sql.CharacterStringType:
			fd.DataType = storage.TypeVarchar
			fd.Len = t.Len
		default:
			panic("unsupported column definition type")
		}
		r.Fields = append(r.Fields, fd)
	}

	path, err := DBPath(db)
	if err != nil {
		return err
	}

	return storage.CreateTable(path, r, q.Name)
}
