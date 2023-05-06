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

func TestGetDataTypes(t *testing.T) {
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
	}
	cfg := importCfg{
		table:   "author",
		dstCols: []string{"name", "age"},
	}

	types, err := getDataTypes(rm, cfg.table, cfg.dstCols)
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
			importedRows = append(importedRows, vals)
			return []*storage.WALEntry{}, nil
		},
		flushWALBatch: func(batch storage.WALBatch) error {
			return nil
		},
	}
	cfg := importCfg{
		table:   "author",
		dstCols: []string{"name", "age"},
		srcCols: []int{0, 1},
		dataTypes: []storage.DataType{
			storage.TypeVarchar,
			storage.TypeInt,
		},
		separator: '\t',
	}

	goodRows := []string {
		"Mike\t10\n" +
		"Malfo\"rmed\tRecord\n" + // should cause ErrBareQuote
		"Joe\t15\tFooBar\n" + // should not cause cause ErrFieldCount
		"Jay\t20\n" +
		"Jay\tFoo\n" + // should cause string conversion error
		"Casey\n" + // should cause "index not present in row"
		"Malfo\"rmed\tRecord\n" + // should cause ErrBareQuote
		"Garv\t\\N"
	}

	badRows := []string {
		"Mike\t10\n" +
		"Malfo\"rmed\tRecord\n" + // should cause ErrBareQuote
		"Joe\t15\tFooBar\n" + // should not cause cause ErrFieldCount
		"Jay\t20\n" +
		"Jay\tFoo\n" + // should cause string conversion error
		"Casey\n" + // should cause "index not present in row"
		"Malfo\"rmed\tRecord\n" + // should cause ErrBareQuote
		"Garv\t\\N"
	}
	csv := "Mike\t10\n" +
		"Malfo\"rmed\tRecord\n" + // should cause ErrBareQuote
		"Joe\t15\tFooBar\n" + // should not cause cause ErrFieldCount
		"Jay\t20\n" +
		"Jay\tFoo\n" + // should cause string conversion error
		"Casey\n" + // should cause "index not present in row"
		"Malfo\"rmed\tRecord\n" + // should cause ErrBareQuote
		"Garv\t\\N"

	chOk, chErr := CSVImport(rm, cfg, bytes.NewBufferString(csv))

	totalOk := 0
	totalCsvParseErr := 0

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
				if errors.Is(err, ErrCsvImport) {
					totalCsvParseErr++
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
		{"Mike", int32(10)},
		{"Joe", int32(15)},
		{"Jay", int32(20)},
		{"Garv", nil},
	}

	if !reflect.DeepEqual(expected, importedRows) {
		t.Fatalf("imported rows do not match expected rows. expected: %v actual: %v", expected, importedRows)
	}

	if totalOk != len(expected) {
		t.Fatalf("total imported does not match expected count. expected: %d actual: %d", len(expected), totalOk)
	}

	expectCsvParseErrCount := 4
	if totalCsvParseErr != expectCsvParseErrCount {
		t.Fatalf("total csv errors does not match expected count. expected: %d actual: %d", expectCsvParseErrCount, totalCsvParseErr)
	}
}
