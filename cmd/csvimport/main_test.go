package main

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/mkaminski/bkdb/storage"
)

type mockRelationManager struct {
	createTable   func(r *storage.Relation, tableName string) error
	markDeleted   func(tableName string, rowID uint32) (storage.WALBatch, error)
	fetch         func(tableName string) ([]*storage.Row, []*storage.Field, error)
	update        func(tableName string, rowID uint32, cols []string, updateSrc []interface{}) (storage.WALBatch, error)
	insert        func(tableName string, cols []string, vals []interface{}) (storage.WALBatch, error)
	flushWALBatch func(batch storage.WALBatch) error
}

func (m *mockRelationManager) CreateTable(r *storage.Relation, tableName string) error {
	return m.createTable(r, tableName)
}
func (m *mockRelationManager) MarkDeleted(tableName string, rowID uint32) (storage.WALBatch, error) {
	return m.markDeleted(tableName, rowID)
}
func (m *mockRelationManager) Fetch(tableName string) ([]*storage.Row, []*storage.Field, error) {
	return m.fetch(tableName)
}
func (m *mockRelationManager) Update(tableName string, rowID uint32, cols []string, updateSrc []interface{}) (storage.WALBatch, error) {
	return m.update(tableName, rowID, cols, updateSrc)
}
func (m *mockRelationManager) Insert(tableName string, cols []string, vals []interface{}) (storage.WALBatch, error) {
	return m.insert(tableName, cols, vals)
}
func (m *mockRelationManager) FlushWALBatch(batch storage.WALBatch) error {
	return m.flushWALBatch(batch)
}

func (m *mockRelationManager) StartTxn() {
}

func (m *mockRelationManager) EndTxn() {
}

func TestImport(t *testing.T) {

	var inserted interface{}

	rm := &mockRelationManager{
		fetch: func(tableName string) ([]*storage.Row, []*storage.Field, error) {
			if tableName != "sys_schema" {
				return nil, nil, errors.New("expected fetch for `sys_schema`")
			}
			return []*storage.Row{
					{Vals: []interface{}{"author", "name", int32(storage.TypeVarchar)}},
					{Vals: []interface{}{"author", "age", int32(storage.TypeInt)}},
				},
				[]*storage.Field{
					{Column: "table_name"},
					{Column: "field_name"},
					{Column: "field_type"},
				},
				nil
		},
		insert: func(tableName string, cols []string, vals []interface{}) (storage.WALBatch, error) {
			inserted = vals
			return []*storage.WALEntry{}, nil
		},
		flushWALBatch: func(batch storage.WALBatch) error {
			return nil
		},
	}
	cfg := importCfg{
		table:   "author",
		dstCols: []string{"name", "age"},
		srcCols: []int{1, 2},
	}

	csv := "Mike\t10\n" +
		"Jay\t20\n" +
		"Garv\t30"

	if err := Import(rm, cfg, bytes.NewBufferString(csv)); err != nil {
		if err != nil {
			t.Fatalf("err getting data types: %s", err.Error())
		}
	}

	expected := []interface{}{}

	if !reflect.DeepEqual(expected, inserted) {
		t.Fatalf("types do not match. expected: %v actual: %v", expected, inserted)
	}
	// types, err := getDataTypes(rm, cfg.table, cfg.dstCols)
	// if err != nil {
	// 	t.Fatalf("err getting data types: %s", err.Error())
	// }

	// expect := []storage.DataType{
	// 	storage.TypeVarchar,
	// 	storage.TypeInt,
	// }

	// if !reflect.DeepEqual(types, expect) {
	// 	t.Fatalf("types do not match. expected: %v actual: %v", expect, types)
	// }
}
