package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func TestSelect(t *testing.T) {

	tc := []struct {
		name         string
		query        sql.Select
		givenFields  btree.Fields
		expectFields []*btree.Field
		givenRows    []*btree.Row
		expectRows   []*btree.Row
		expectErr    error
	}{
		{
			name: "SELECT with ORDER BY",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				SortSpecificationList: []sql.SortSpecification{
					{
						SortKey: sql.ValueExpression{
							Qualifier: nil,
							ColumnName: sql.Token{
								Type: sql.IDENT,
								Text: "col1",
							},
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
					{
						SortKey: sql.ValueExpression{
							Qualifier: nil,
							ColumnName: sql.Token{
								Type: sql.IDENT,
								Text: "col2",
							},
						},
						OrderingSpecification: sql.Token{Type: sql.ASC},
					},
					{
						SortKey: sql.ValueExpression{
							Qualifier: nil,
							ColumnName: sql.Token{
								Type: sql.IDENT,
								Text: "col3",
							},
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
				},
			},
			givenFields: btree.Fields{
				&btree.Field{Column: "col1"},
				&btree.Field{Column: "col2"},
				&btree.Field{Column: "col3"},
			},
			expectFields: []*btree.Field{
				{Column: "col1", TableID: "the_table"},
				{Column: "col2", TableID: "the_table"},
				{Column: "col3", TableID: "the_table"},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"c", int32(1), int32(8)}},
				{Vals: []interface{}{"c", int32(5), int32(4)}},
				{Vals: []interface{}{"c", int32(5), int32(5)}},
				{Vals: []interface{}{"b", int32(5), int32(5)}},
				{Vals: []interface{}{"b", int32(6), int32(6)}},
				{Vals: []interface{}{"b", int32(7), int32(7)}},
				{Vals: []interface{}{"a", int32(2), int32(2)}},
				{Vals: []interface{}{"a", int32(3), int32(3)}},
				{Vals: []interface{}{"a", int32(5), int32(5)}},
			},
			expectRows: []*btree.Row{
				{Vals: []interface{}{"c", int32(1), int32(8)}},
				{Vals: []interface{}{"c", int32(5), int32(5)}},
				{Vals: []interface{}{"c", int32(5), int32(4)}},
				{Vals: []interface{}{"b", int32(5), int32(5)}},
				{Vals: []interface{}{"b", int32(6), int32(6)}},
				{Vals: []interface{}{"b", int32(7), int32(7)}},
				{Vals: []interface{}{"a", int32(2), int32(2)}},
				{Vals: []interface{}{"a", int32(3), int32(3)}},
				{Vals: []interface{}{"a", int32(5), int32(5)}},
			},
		},
		{
			name: "SELECT with ORDER BY, second column contains identical values",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				SortSpecificationList: []sql.SortSpecification{
					{
						SortKey: sql.ValueExpression{
							Qualifier: nil,
							ColumnName: sql.Token{
								Type: sql.IDENT,
								Text: "col1",
							},
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
					{
						SortKey: sql.ValueExpression{
							Qualifier: nil,
							ColumnName: sql.Token{
								Type: sql.IDENT,
								Text: "col2",
							},
						},
						OrderingSpecification: sql.Token{Type: sql.ASC},
					},
					{
						SortKey: sql.ValueExpression{
							Qualifier: nil,
							ColumnName: sql.Token{
								Type: sql.IDENT,
								Text: "col3",
							},
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
				},
			},
			givenFields: btree.Fields{
				&btree.Field{Column: "col1"},
				&btree.Field{Column: "col2"},
				&btree.Field{Column: "col3"},
			},
			expectFields: []*btree.Field{
				{Column: "col1", TableID: "the_table"},
				{Column: "col2", TableID: "the_table"},
				{Column: "col3", TableID: "the_table"},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"a", int32(1), int32(4)}},
				{Vals: []interface{}{"a", int32(1), int32(5)}},
				{Vals: []interface{}{"a", int32(1), int32(6)}},
				{Vals: []interface{}{"b", int32(1), int32(1)}},
				{Vals: []interface{}{"b", int32(1), int32(2)}},
				{Vals: []interface{}{"b", int32(1), int32(3)}},
			},
			expectRows: []*btree.Row{
				{Vals: []interface{}{"b", int32(1), int32(3)}},
				{Vals: []interface{}{"b", int32(1), int32(2)}},
				{Vals: []interface{}{"b", int32(1), int32(1)}},
				{Vals: []interface{}{"a", int32(1), int32(6)}},
				{Vals: []interface{}{"a", int32(1), int32(5)}},
				{Vals: []interface{}{"a", int32(1), int32(4)}},
			},
		},
		{
			name: "SELECT with ORDER BY on non-existent field",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				SortSpecificationList: []sql.SortSpecification{
					{
						SortKey: sql.ValueExpression{
							Qualifier: nil,
							ColumnName: sql.Token{
								Type: sql.IDENT,
								Text: "non_existent_col",
							},
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
				},
			},
			givenFields: btree.Fields{
				&btree.Field{Column: "col1"},
			},
			expectFields: nil,
			givenRows: []*btree.Row{
				{Vals: []interface{}{"a"}},
				{Vals: []interface{}{"b"}},
				{Vals: []interface{}{"c"}},
				{Vals: []interface{}{"d"}},
			},
			expectRows: nil,
			expectErr:  ErrSortFieldNotFound,
		},
		{
			name: "SELECT with LIMIT value that exceeds result set size",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					LimitActive: true,
					Limit:       100,
				},
			},
			givenFields: btree.Fields{
				&btree.Field{Column: "col1"},
			},
			expectFields: []*btree.Field{
				{Column: "col1", TableID: "the_table"},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			name: "SELECT with LIMIT value within size of result set",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					LimitActive: true,
					Limit:       2,
				},
			},
			givenFields: btree.Fields{
				&btree.Field{Column: "col1"},
			},
			expectFields: []*btree.Field{
				{Column: "col1", TableID: "the_table"},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
			},
		},
		{
			name: "SELECT with LIMIT value 0",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					LimitActive: true,
					Limit:       0,
				},
			},
			givenFields: btree.Fields{
				&btree.Field{Column: "col1"},
			},
			expectFields: []*btree.Field{
				{Column: "col1", TableID: "the_table"},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{},
		},
		{
			name: "SELECT with OFFSET value 0",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       0,
				},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			name: "SELECT with OFFSET value within size of result set",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       2,
				},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			name: "SELECT with OFFSET value equal to result set size",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       4,
				},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{},
		},
		{
			name: "SELECT with OFFSET value that exceeds result set size",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       100,
				},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{},
		},
		{
			name: "SELECT with LIMIT and OFFSET",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "the_table",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					LimitActive:  true,
					OffsetActive: true,
					Limit:        2,
					Offset:       1,
				},
			},
			givenRows: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expectRows: []*btree.Row{
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
			},
		},
	}

	for _, test := range tc {
		t.Run(test.name, func(t *testing.T) {
			fetcher := func(path string, tableName string) ([]*btree.Row, []*btree.Field, error) {
				return test.givenRows, test.givenFields, nil
			}

			actualRows, actualFields, err := EvaluateSelect(test.query, "", fetcher)

			if !errors.Is(err, test.expectErr) {
				t.Errorf("expected error `%v`, got `%v`", test.expectErr, err)
			}

			if !reflect.DeepEqual(test.expectRows, actualRows) {
				t.Fatalf("rows do not match. expected: %s actual: %s", test.expectRows, actualRows)
			}

			if !reflect.DeepEqual(test.expectFields, actualFields) {
				t.Fatalf("fields do not match. expected: %s actual: %s", test.expectFields, actualFields)
			}
		})
	}
}
