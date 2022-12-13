package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func TestDelete(t *testing.T) {

	tc := []struct {
		name          string
		query         sql.DeleteStatementSearched
		givenFields   map[string]btree.Fields
		givenRows     map[string][]*btree.Row
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
							LHS: sql.ValueExpression{
								ColumnName: sql.Token{
									Type:   sql.IDENT,
									Line:   0,
									Column: 0,
									Text:   "val",
								},
							},
							CompOp: sql.EQ,
							RHS: sql.ValueExpression{
								ColumnName: sql.Token{
									Type:   sql.STR,
									Line:   0,
									Column: 0,
									Text:   "c",
								},
							},
						},
					},
				},
			},
			givenFields: map[string]btree.Fields{
				"tbl1": {
					&btree.Field{Column: "val"},
				},
			},
			givenRows: map[string][]*btree.Row{
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
			fetcher := func(path string, tableName string) ([]*btree.Row, []*btree.Field, error) {
				return test.givenRows[tableName], test.givenFields[tableName], nil
			}
			var actualDeleted []int32
			deleter := func(path string, tableName string, rowID uint32) error {
				actualDeleted = append(actualDeleted, int32(rowID))
				return nil
			}

			count, err := EvaluateDelete(test.query, "", fetcher, deleter)

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
