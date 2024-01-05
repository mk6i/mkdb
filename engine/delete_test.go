package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mk6i/mkdb/sql"
	"github.com/mk6i/mkdb/storage"
)

func TestDelete(t *testing.T) {

	tc := []struct {
		name          string
		query         sql.DeleteStatementSearched
		givenFields   map[string]storage.Fields
		givenRows     map[string][]*storage.Row
		expectDeleted []int32
		expectErr     error
	}{
		{
			name: "DELETE with WHERE clause: DELETE FROM tbl1 WHERE val = 'c'",
			query: sql.DeleteStatementSearched{
				TableName: "tbl1",
				WhereClause: sql.WhereClause{
					SearchCondition: sql.Predicate{
						ComparisonPredicate: sql.ComparisonPredicate{
							LHS: sql.ColumnReference{
								ColumnName: "val",
							},
							CompOp: sql.EQ,
							RHS:    "c",
						},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "val"},
				},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{RowID: 1, Vals: []interface{}{"a"}},
					{RowID: 2, Vals: []interface{}{"b"}},
					{RowID: 3, Vals: []interface{}{"c"}},
					{RowID: 4, Vals: []interface{}{"d"}},
					{RowID: 5, Vals: []interface{}{"c"}},
					{RowID: 6, Vals: []interface{}{"f"}},
					{RowID: 7, Vals: []interface{}{"c"}},
					{RowID: 8, Vals: []interface{}{"h"}},
				},
			},
			expectDeleted: []int32{3, 5, 7},
		},
	}

	for _, test := range tc {
		t.Run(test.name, func(t *testing.T) {
			var actualDeleted []int32

			rm := &mockRelationManager{
				fetch: func(tableName string) ([]*storage.Row, []*storage.Field, error) {
					return test.givenRows[tableName], test.givenFields[tableName], nil
				},
				markDeleted: func(tableName string, rowID uint32) (storage.WALBatch, error) {
					actualDeleted = append(actualDeleted, int32(rowID))
					return nil, nil
				},
				flushWALBatch: func(batch storage.WALBatch) error {
					return nil
				},
			}

			count, err := EvaluateDelete(test.query, rm)

			if !errors.Is(err, test.expectErr) {
				t.Errorf("expected error `%v`, got `%v`", test.expectErr, err)
			}
			if len(test.expectDeleted) != count {
				t.Fatalf("deleted count does not match. expected: %d actual: %d", len(test.expectDeleted), count)
			}
			if !reflect.DeepEqual(test.expectDeleted, actualDeleted) {
				t.Fatalf("deleted row ID list does not match. expected: %v actual: %v", test.expectDeleted, actualDeleted)
			}
		})
	}
}
