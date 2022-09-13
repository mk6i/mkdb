package engine

import (
	"reflect"
	"testing"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func TestSortColumns(t *testing.T) {

	tc := []struct {
		ssl     []sql.SortSpecification
		qfields btree.Fields
		given   []*btree.Row
		expect  []*btree.Row
	}{
		{
			ssl: []sql.SortSpecification{
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
			qfields: btree.Fields{
				&btree.Field{Column: "col1"},
				&btree.Field{Column: "col2"},
				&btree.Field{Column: "col3"},
			},
			given: []*btree.Row{
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
			expect: []*btree.Row{
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
			ssl: []sql.SortSpecification{
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
			qfields: btree.Fields{
				&btree.Field{Column: "col1"},
				&btree.Field{Column: "col2"},
				&btree.Field{Column: "col3"},
			},
			given: []*btree.Row{
				{Vals: []interface{}{"a", int32(1), int32(4)}},
				{Vals: []interface{}{"a", int32(1), int32(5)}},
				{Vals: []interface{}{"a", int32(1), int32(6)}},
				{Vals: []interface{}{"b", int32(1), int32(1)}},
				{Vals: []interface{}{"b", int32(1), int32(2)}},
				{Vals: []interface{}{"b", int32(1), int32(3)}},
			},
			expect: []*btree.Row{
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
		if err := sortColumns(test.ssl, test.qfields, test.given); err != nil {
			t.Fatalf("sortColumns failure: %s", err.Error())
		}
		if !reflect.DeepEqual(test.expect, test.given) {
			t.Errorf("columns not sorted as expected. \ngiven:\n%v, \nactual:\n%v\n", test.expect, test.given)
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
