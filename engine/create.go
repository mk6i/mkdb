package engine

import (
	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

func EvaluateCreateDatabase(q sql.CreateDatabase) error {
	return storage.CreateDB(q.Name)
}

func EvaluateCreateTable(q sql.CreateTable, rm relationManager) error {
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

	return rm.CreateTable(r, q.Name)
}
