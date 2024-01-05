package main

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mk6i/mkdb/storage"
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

func TestGetDataTypes(t *testing.T) {
	rm := &mockRelationManager{
		fetch: func(tableName string) ([]*storage.Row, []*storage.Field, error) {
			if tableName != "sys_schema" {
				return nil, nil, errors.New("expected fetch for `sys_schema`")
			}
			return []*storage.Row{
					{Vals: []interface{}{"author", "name", int64(storage.TypeVarchar)}},
					{Vals: []interface{}{"author", "age", int64(storage.TypeInt)}},
				},
				[]*storage.Field{
					{Column: "table_name"},
					{Column: "field_name"},
					{Column: "field_type"},
				},
				nil
		},
	}
	cfg := importCfg{
		table:   "author",
		dstCols: []string{"name", "age"},
	}

	types, err := colDataTypes(rm, cfg.table, cfg.dstCols)
	if err != nil {
		t.Fatalf("err getting data types: %s", err.Error())
	}

	expect := []storage.DataType{
		storage.TypeVarchar,
		storage.TypeInt,
	}

	if !reflect.DeepEqual(types, expect) {
		t.Fatalf("types do not match. expected: %v actual: %v", expect, types)
	}
}

func TestCSVImport(t *testing.T) {

	var importedRows [][]interface{}

	rm := &mockRelationManager{
		fetch: func(tableName string) ([]*storage.Row, []*storage.Field, error) {
			if tableName != "sys_schema" {
				return nil, nil, errors.New("expected fetch for `sys_schema`")
			}
			return []*storage.Row{
					{Vals: []interface{}{"author", "name", int64(storage.TypeVarchar)}},
					{Vals: []interface{}{"author", "age", int64(storage.TypeInt)}},
					{Vals: []interface{}{"author", "active", int64(storage.TypeBoolean)}},
				},
				[]*storage.Field{
					{Column: "table_name"},
					{Column: "field_name"},
					{Column: "field_type"},
				},
				nil
		},
		insert: func(tableName string, cols []string, vals []interface{}) (storage.WALBatch, error) {
			importedRows = append(importedRows, vals)
			return []*storage.WALEntry{}, nil
		},
		flushWALBatch: func(batch storage.WALBatch) error {
			return nil
		},
	}
	cfg := importCfg{
		table:   "author",
		dstCols: []string{"name", "age", "active"},
		srcCols: []int{0, 1, 2},
		colTypes: []storage.DataType{
			storage.TypeVarchar,
			storage.TypeInt,
			storage.TypeBoolean,
		},
		separator: ',',
	}

	goodRows := []string{
		strings.Join([]string{"Person1", "10", "true"}, string(cfg.separator)),
		strings.Join([]string{"Person2", "20", "false"}, string(cfg.separator)),
		strings.Join([]string{"Person3", "\\N", "true"}, string(cfg.separator)),
		strings.Join([]string{"Person5", "15", "false", "FooBar"}, string(cfg.separator)),
	}
	badRows := []string{
		strings.Join([]string{"Per\"son4", "30", "true"}, string(cfg.separator)), // should cause ErrBareQuote
		strings.Join([]string{"Person6", "Foo", "true"}, string(cfg.separator)),  // should cause string conversion error on second column
		strings.Join([]string{"Person7"}, string(cfg.separator)),                 // should cause "index not present in row"
	}
	csv := strings.Join(append(goodRows, badRows...), "\n")

	chOk, chErr := doBatchInsert(rm, cfg, bytes.NewBufferString(csv))

	totalOk := 0
	totalErr := 0

	for {
		select {
		case _, ok := <-chOk:
			if ok {
				totalOk++
			} else {
				chOk = nil
			}
		case err, ok := <-chErr:
			if ok {
				if errors.Is(err, errMalformedRow) {
					totalErr++
				} else {
					t.Fatalf("unexpected error: %s", err.Error())
				}
			} else {
				chErr = nil
			}
		}
		if chOk == nil && chErr == nil {
			break
		}
	}

	expected := [][]interface{}{
		{"Person1", int64(10), true},
		{"Person2", int64(20), false},
		{"Person3", nil, true},
		{"Person5", int64(15), false},
	}

	if !reflect.DeepEqual(expected, importedRows) {
		t.Fatalf("imported rows do not match expected rows. expected: %v actual: %v", expected, importedRows)
	}
	if totalOk != len(goodRows) {
		t.Fatalf("total imported does not match expected count. expected: %d actual: %d", len(goodRows), totalOk)
	}
	if totalErr != len(badRows) {
		t.Fatalf("total csv errors does not match expected count. expected: %d actual: %d", len(badRows), totalErr)
	}
}
