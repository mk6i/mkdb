package engine

import (
	"reflect"
	"testing"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func TestSelectStar(t *testing.T) {

	query := sql.Select{
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
	}

	givenFields := btree.Fields{
		&btree.Field{Column: "col1"},
		&btree.Field{Column: "col2"},
	}
	expectFields := []*btree.Field{
		{Column: "col1", TableID: "the_table"},
		{Column: "col2", TableID: "the_table"},
	}
	givenRows := []*btree.Row{
		{Vals: []interface{}{"a", int32(1)}},
		{Vals: []interface{}{"b", int32(2)}},
		{Vals: []interface{}{"c", int32(3)}},
	}
	expectRows := []*btree.Row{
		{Vals: []interface{}{"a", int32(1)}},
		{Vals: []interface{}{"b", int32(2)}},
		{Vals: []interface{}{"c", int32(3)}},
	}

	fetcher := func(path string, tableName string) ([]*btree.Row, []*btree.Field, error) {
		return givenRows, givenFields, nil
	}

	actualRows, actualFields, err := EvaluateSelect(query, "", fetcher)
	if err != nil {
		t.Fatalf("encountered error on select *: %s", err.Error())
	}

	if !reflect.DeepEqual(expectRows, actualRows) {
		t.Fatalf("rows do not match. expected: %s actual: %s", expectRows, actualRows)
	}

	if !reflect.DeepEqual(expectFields, actualFields) {
		t.Fatalf("fields do not match. expected: %s actual: %s", expectFields, actualFields)
	}
}

func TestSelectOrderBy(t *testing.T) {

	tc := []struct {
		query        sql.Select
		givenFields  btree.Fields
		expectFields []*btree.Field
		givenRows    []*btree.Row
		expectRows   []*btree.Row
	}{
		{
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
	}

	for _, test := range tc {

		fetcher := func(path string, tableName string) ([]*btree.Row, []*btree.Field, error) {
			return test.givenRows, test.givenFields, nil
		}

		actualRows, actualFields, err := EvaluateSelect(test.query, "", fetcher)
		if err != nil {
			t.Fatalf("encountered error on select *: %s", err.Error())
		}

		if !reflect.DeepEqual(test.expectRows, actualRows) {
			t.Fatalf("rows do not match. expected: %s actual: %s", test.expectRows, actualRows)
		}

		if !reflect.DeepEqual(test.expectFields, actualFields) {
			t.Fatalf("fields do not match. expected: %s actual: %s", test.expectFields, actualFields)
		}
	}
}

func TestLimit(t *testing.T) {
	tc := []struct {
		limit  int
		given  []*btree.Row
		expect []*btree.Row
	}{
		{
			limit: 100,
			given: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expect: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			limit: 2,
			given: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expect: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
			},
		},
		{
			limit: 0,
			given: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expect: []*btree.Row{},
		},
	}

	for _, test := range tc {
		actual := limit(test.limit, test.given)
		if !reflect.DeepEqual(test.expect, actual) {
			t.Errorf("result not limited as expected. \nexpected:\n%v, \nactual:\n%v\n", test.expect, actual)
		}
	}
}

func TestOffset(t *testing.T) {
	tc := []struct {
		offset int
		given  []*btree.Row
		expect []*btree.Row
	}{
		{
			offset: 0,
			given: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expect: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			offset: 2,
			given: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expect: []*btree.Row{
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			offset: 4,
			given: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expect: []*btree.Row{},
		},
		{
			offset: 100,
			given: []*btree.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
			expect: []*btree.Row{},
		},
	}

	for _, test := range tc {
		actual := offset(test.offset, test.given)
		if !reflect.DeepEqual(test.expect, actual) {
			t.Errorf("result not limited as expected. \nexpected:\n%v, \nactual:\n%v\n", test.expect, actual)
		}
	}
}
