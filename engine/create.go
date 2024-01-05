package engine

import (
	"github.com/mk6i/mkdb/sql"
	"github.com/mk6i/mkdb/storage"
)

func EvaluateCreateDatabase(q sql.CreateDatabase) error {
	return storage.CreateDB(q.Name)
}

func EvaluateCreateTable(q sql.CreateTable, rm RelationManager) error {
	r := &storage.Relation{}

	for _, elem := range q.Elements {
		fd := storage.FieldDef{
			Name: elem.ColumnDefinition.Name,
		}
		switch t := elem.ColumnDefinition.DataType.(type) {
		case sql.NumericType:
			fd.DataType = storage.TypeInt
		case sql.BigIntType:
			fd.DataType = storage.TypeBigInt
		case sql.CharacterStringType:
			fd.DataType = storage.TypeVarchar
			fd.Len = t.Len
		case sql.BooleanType:
			fd.DataType = storage.TypeBoolean
		default:
			panic("unsupported column definition type")
		}
		r.Fields = append(r.Fields, fd)
	}

	return rm.CreateTable(r, q.Name)
}
