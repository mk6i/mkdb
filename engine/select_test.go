package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mkaminski/bkdb/sql"
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

func TestSelect(t *testing.T) {

	tc := []struct {
		name         string
		query        sql.Select
		givenFields  map[string]storage.Fields
		expectFields []*storage.Field
		givenRows    map[string][]*storage.Row
		expectRows   []*storage.Row
		expectErr    error
	}{
		{
			name: "SELECT with ORDER BY: SELECT * FROM tbl1 ORDER BY col1 DESC, col2 ASC, col3 DESC",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				SortSpecificationList: []sql.SortSpecification{
					{
						SortKey: sql.ColumnReference{
							Qualifier:  "",
							ColumnName: "col1",
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
					{
						SortKey: sql.ColumnReference{
							Qualifier:  "",
							ColumnName: "col2",
						},
						OrderingSpecification: sql.Token{Type: sql.ASC},
					},
					{
						SortKey: sql.ColumnReference{
							Qualifier:  "",
							ColumnName: "col3",
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
					&storage.Field{Column: "col2"},
					&storage.Field{Column: "col3"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
				{Column: "col2", TableID: "tbl1"},
				{Column: "col3", TableID: "tbl1"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"c", 1, 8}},
					{Vals: []interface{}{"c", 5, 4}},
					{Vals: []interface{}{"c", 5, 5}},
					{Vals: []interface{}{"b", 5, 5}},
					{Vals: []interface{}{"b", 6, 6}},
					{Vals: []interface{}{"b", 7, 7}},
					{Vals: []interface{}{"a", 2, 2}},
					{Vals: []interface{}{"a", 3, 3}},
					{Vals: []interface{}{"a", 5, 5}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"c", 1, 8}},
				{Vals: []interface{}{"c", 5, 5}},
				{Vals: []interface{}{"c", 5, 4}},
				{Vals: []interface{}{"b", 5, 5}},
				{Vals: []interface{}{"b", 6, 6}},
				{Vals: []interface{}{"b", 7, 7}},
				{Vals: []interface{}{"a", 2, 2}},
				{Vals: []interface{}{"a", 3, 3}},
				{Vals: []interface{}{"a", 5, 5}},
			},
		},
		{
			name: "SELECT with ORDER BY, 2nd col has identical values: SELECT * FROM tbl1 ORDER BY col1 DESC, col2 ASC, col3 DESC",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				SortSpecificationList: []sql.SortSpecification{
					{
						SortKey: sql.ColumnReference{
							Qualifier:  "",
							ColumnName: "col1",
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
					{
						SortKey: sql.ColumnReference{
							Qualifier:  "",
							ColumnName: "col2",
						},
						OrderingSpecification: sql.Token{Type: sql.ASC},
					},
					{
						SortKey: sql.ColumnReference{
							Qualifier:  "",
							ColumnName: "col3",
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
					&storage.Field{Column: "col2"},
					&storage.Field{Column: "col3"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
				{Column: "col2", TableID: "tbl1"},
				{Column: "col3", TableID: "tbl1"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"a", 1, 4}},
					{Vals: []interface{}{"a", 1, 5}},
					{Vals: []interface{}{"a", 1, 6}},
					{Vals: []interface{}{"b", 1, 1}},
					{Vals: []interface{}{"b", 1, 2}},
					{Vals: []interface{}{"b", 1, 3}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"b", 1, 3}},
				{Vals: []interface{}{"b", 1, 2}},
				{Vals: []interface{}{"b", 1, 1}},
				{Vals: []interface{}{"a", 1, 6}},
				{Vals: []interface{}{"a", 1, 5}},
				{Vals: []interface{}{"a", 1, 4}},
			},
		},
		{
			name: "SELECT with ORDER BY on non-existent field: SELECT * FROM tbl1 ORDER BY non_existent_col DESC",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				SortSpecificationList: []sql.SortSpecification{
					{
						SortKey: sql.ColumnReference{
							Qualifier:  "",
							ColumnName: "non_existent_col",
						},
						OrderingSpecification: sql.Token{Type: sql.DESC},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
				},
			},
			expectFields: nil,
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"a"}},
					{Vals: []interface{}{"b"}},
					{Vals: []interface{}{"c"}},
					{Vals: []interface{}{"d"}},
				},
			},
			expectRows: nil,
			expectErr:  ErrSortFieldNotFound,
		},
		{
			name: "SELECT with LIMIT value that exceeds result set size: SELECT * FROM tbl1 LIMIT 100",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					LimitActive: true,
					Limit:       100,
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			name: "SELECT with LIMIT value within size of result set: SELECT * FROM tbl1 LIMIT 2",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					LimitActive: true,
					Limit:       2,
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
			},
		},
		{
			name: "SELECT with LIMIT value 0: SELECT * FROM tbl1 LIMIT 0",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					LimitActive: true,
					Limit:       0,
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{},
		},
		{
			name: "SELECT with OFFSET value 0: SELECT * FROM tbl1 OFFSET 100",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       0,
				},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"0"}},
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			name: "SELECT with OFFSET value within size of result set: SELECT * FROM tbl1 OFFSET 2",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       2,
				},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"2"}},
				{Vals: []interface{}{"3"}},
			},
		},
		{
			name: "SELECT with OFFSET value equal to result set size: SELECT * FROM tbl1 OFFSET 4",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       4,
				},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{},
		},
		{
			name: "SELECT with OFFSET value that exceeds result set size: SELECT * FROM tbl1 OFFSET 100",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
				LimitOffsetClause: sql.LimitOffsetClause{
					OffsetActive: true,
					Offset:       100,
				},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{},
		},
		{
			name: "SELECT with LIMIT and OFFSET: SELECT * FROM tbl1 LIMIT 2 OFFSET 1",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Asterisk{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
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
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"0"}},
					{Vals: []interface{}{"1"}},
					{Vals: []interface{}{"2"}},
					{Vals: []interface{}{"3"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"1"}},
				{Vals: []interface{}{"2"}},
			},
		},
		{
			name: `SELECT with INNER JOIN: SELECT tbl1.col1, tbl2.col3 FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.id`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl1",
							ColumnName: "col1",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl2",
							ColumnName: "col3",
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.QualifiedJoin{
							LHS:      sql.TableName{Name: "tbl1"},
							RHS:      sql.TableName{Name: "tbl2"},
							JoinType: sql.INNER_JOIN,
							JoinCondition: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS: sql.ColumnReference{
										Qualifier:  "tbl1",
										ColumnName: "id",
									},
									CompOp: sql.EQ,
									RHS: sql.ColumnReference{
										Qualifier:  "tbl2",
										ColumnName: "id",
									},
								},
							},
						},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "col1"},
					&storage.Field{Column: "col2"},
				},
				"tbl2": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "col3"},
					&storage.Field{Column: "col4"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
				{Column: "col3", TableID: "tbl2"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"id_+1", 1, 2}},
					{Vals: []interface{}{"id_+2", 3, 4}},
					{Vals: []interface{}{"id_+5", 5, 6}},
					{Vals: []interface{}{"id_+6", 7, 8}},
					{Vals: []interface{}{"id_+7", 9, 10}},
					{Vals: []interface{}{"id_+9", 11, 12}},
					{Vals: []interface{}{"id_+10", 13, 14}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_+1", 15, 16}},
					{Vals: []interface{}{"id_-2", 17, 18}},
					{Vals: []interface{}{"id_+5", 19, 20}},
					{Vals: []interface{}{"id_-6", 21, 22}},
					{Vals: []interface{}{"id_+7", 23, 24}},
					{Vals: []interface{}{"id_-9", 25, 26}},
					{Vals: []interface{}{"id_+10", 27, 28}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{1, 15}},
				{Vals: []interface{}{5, 19}},
				{Vals: []interface{}{9, 23}},
				{Vals: []interface{}{13, 27}},
			},
		},
		{
			name: `SELECT with LEFT JOIN: SELECT tbl1.col1, tbl2.col3 FROM tbl1 LEFT JOIN tbl2 ON tbl1.id = tbl2.id`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl1",
							ColumnName: "col1",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl2",
							ColumnName: "col3",
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.QualifiedJoin{
							LHS:      sql.TableName{Name: "tbl1"},
							RHS:      sql.TableName{Name: "tbl2"},
							JoinType: sql.LEFT_JOIN,
							JoinCondition: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS: sql.ColumnReference{
										Qualifier:  "tbl1",
										ColumnName: "id",
									},
									CompOp: sql.EQ,
									RHS: sql.ColumnReference{
										Qualifier:  "tbl2",
										ColumnName: "id",
									},
								},
							},
						},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "col1"},
					&storage.Field{Column: "col2"},
				},
				"tbl2": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "col3"},
					&storage.Field{Column: "col4"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
				{Column: "col3", TableID: "tbl2"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"id_1", 1, 2}},
					{Vals: []interface{}{"id_2", 3, 4}},
					{Vals: []interface{}{"id_3", 5, 6}},
					{Vals: []interface{}{"id_4", 7, 8}},
					{Vals: []interface{}{"id_5", 9, 10}},
					{Vals: []interface{}{"id_6", 11, 12}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_1", 13, 14}},
					{Vals: []interface{}{"id_3", 15, 16}},
					{Vals: []interface{}{"id_5", 17, 18}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{1, 13}},
				{Vals: []interface{}{3, nil}},
				{Vals: []interface{}{5, 15}},
				{Vals: []interface{}{7, nil}},
				{Vals: []interface{}{9, 17}},
				{Vals: []interface{}{11, nil}},
			},
		},
		{
			name: `SELECT with RIGHT JOIN: SELECT tbl1.col1, tbl2.col3 FROM tbl1 RIGHT JOIN tbl2 ON tbl1.id = tbl2.id`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl1",
							ColumnName: "col1",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl2",
							ColumnName: "col3",
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.QualifiedJoin{
							LHS:      sql.TableName{Name: "tbl1"},
							RHS:      sql.TableName{Name: "tbl2"},
							JoinType: sql.RIGHT_JOIN,
							JoinCondition: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS: sql.ColumnReference{
										Qualifier:  "tbl1",
										ColumnName: "id",
									},
									CompOp: sql.EQ,
									RHS: sql.ColumnReference{
										Qualifier:  "tbl2",
										ColumnName: "id",
									},
								},
							},
						},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "col1"},
					&storage.Field{Column: "col2"},
				},
				"tbl2": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "col3"},
					&storage.Field{Column: "col4"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
				{Column: "col3", TableID: "tbl2"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"id_2", 1, 2}},
					{Vals: []interface{}{"id_4", 3, 4}},
					{Vals: []interface{}{"id_6", 5, 6}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_1", 7, 8}},
					{Vals: []interface{}{"id_2", 9, 10}},
					{Vals: []interface{}{"id_3", 11, 12}},
					{Vals: []interface{}{"id_4", 13, 14}},
					{Vals: []interface{}{"id_5", 15, 16}},
					{Vals: []interface{}{"id_6", 17, 18}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{nil, 7}},
				{Vals: []interface{}{1, 9}},
				{Vals: []interface{}{nil, 11}},
				{Vals: []interface{}{3, 13}},
				{Vals: []interface{}{nil, 15}},
				{Vals: []interface{}{5, 17}},
			},
		},
		{
			name: `SELECT expression with FROM clause: SELECT col1, col1=id_2 FROM tbl1`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							ColumnName: "col1",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Predicate{
							ComparisonPredicate: sql.ComparisonPredicate{
								LHS: sql.ColumnReference{
									ColumnName: "col1",
								},
								CompOp: sql.EQ,
								RHS:    "id_2",
							},
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{Name: "tbl1"},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "col1", TableID: "tbl1"},
				{Column: "?"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"id_1"}},
					{Vals: []interface{}{"id_2"}},
					{Vals: []interface{}{"id_3"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"id_1", false}},
				{Vals: []interface{}{"id_2", true}},
				{Vals: []interface{}{"id_3", false}},
			},
		},
		{
			name: `SELECT comparison predicate without FROM clause: SELECT 1=1, 1=2`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Predicate{
							ComparisonPredicate: sql.ComparisonPredicate{
								LHS:    1,
								CompOp: sql.EQ,
								RHS:    1,
							},
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Predicate{
							ComparisonPredicate: sql.ComparisonPredicate{
								LHS:    1,
								CompOp: sql.EQ,
								RHS:    2,
							},
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: []sql.TableReference{},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "?"},
				{Column: "?"},
			},
			givenRows: map[string][]*storage.Row{},
			expectRows: []*storage.Row{
				{Vals: []interface{}{true, false}},
			},
		},
		{
			name: `SELECT boolean term without FROM clause: SELECT 1=1 AND 2=2`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.BooleanTerm{
							LHS: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS:    1,
									CompOp: sql.EQ,
									RHS:    1,
								},
							},
							RHS: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS:    2,
									CompOp: sql.EQ,
									RHS:    2,
								},
							},
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: []sql.TableReference{},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "col1"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "?"},
			},
			givenRows: map[string][]*storage.Row{},
			expectRows: []*storage.Row{
				{Vals: []interface{}{true}},
			},
		},
		{
			name: `SELECT expression with FROM clause: SELECT field_1 as field_1_alias, field_2 field_2_alias FROM tbl1`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							ColumnName: "field_1",
						},
						AsClause: "field_1_alias",
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							ColumnName: "field_2",
						},
						AsClause: "field_2_alias",
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{Name: "tbl1"},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "field_1"},
					&storage.Field{Column: "field_2"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "field_1_alias", TableID: "tbl1"},
				{Column: "field_2_alias", TableID: "tbl1"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"id_1", "id_2"}},
					{Vals: []interface{}{"id_3", "id_4"}},
					{Vals: []interface{}{"id_5", "id_6"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"id_1", "id_2"}},
				{Vals: []interface{}{"id_3", "id_4"}},
				{Vals: []interface{}{"id_5", "id_6"}},
			},
		},
		{
			name: `select with aggregation on two fields: SELECT customer_id, product_id, count(*) as total
		        FROM orders
		        GROUP BY customer_id, product_id`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							ColumnName: "customer_id",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							ColumnName: "product_id",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Count{},
						AsClause:               "total",
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{Name: "orders"},
					},
					GroupByClause: []sql.ColumnReference{
						{ColumnName: "customer_id"},
						{ColumnName: "product_id"},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"orders": {
					&storage.Field{Column: "customer_id"},
					&storage.Field{Column: "product_id"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "customer_id", TableID: "orders"},
				{Column: "product_id", TableID: "orders"},
				{Column: "total"},
			},
			givenRows: map[string][]*storage.Row{
				"orders": {
					{Vals: []interface{}{"1", "A"}},
					{Vals: []interface{}{"1", "A"}},
					{Vals: []interface{}{"1", "B"}},
					{Vals: []interface{}{"2", "B"}},
					{Vals: []interface{}{"2", "B"}},
					{Vals: []interface{}{"2", "C"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"1", "A", 2}},
				{Vals: []interface{}{"1", "B", 1}},
				{Vals: []interface{}{"2", "B", 2}},
				{Vals: []interface{}{"2", "C", 1}},
			},
		},
		{
			name: "select with implicit aggregation returns non-empty result set: SELECT count(*) FROM tbl1",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Count{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{1}},
					{Vals: []interface{}{2}},
					{Vals: []interface{}{3}},
					{Vals: []interface{}{4}},
				},
			},
			expectFields: []*storage.Field{
				{Column: "count(*)"},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{4}},
			},
		},
		{
			name: "implicit aggregation count(*) and scalar expression with empty result set: SELECT 1, count(*) FROM tbl1",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: 1,
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Count{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "tbl1",
						},
					},
				},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {},
			},
			expectFields: []*storage.Field{
				{Column: "?"},
				{Column: "count(*)"},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{1, 0}},
			},
		},
		{
			name: `select aggregate with column qualifier: SELECT tbl2.year, count(*)
		        FROM tbl1
		        JOIN tbl2 ON tbl1.id = tbl2.id
		        GROUP BY year`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl2",
							ColumnName: "year",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Count{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.QualifiedJoin{
							LHS:      sql.TableName{Name: "tbl1"},
							RHS:      sql.TableName{Name: "tbl2"},
							JoinType: sql.INNER_JOIN,
							JoinCondition: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS: sql.ColumnReference{
										Qualifier:  "tbl1",
										ColumnName: "id",
									},
									CompOp: sql.EQ,
									RHS: sql.ColumnReference{
										Qualifier:  "tbl2",
										ColumnName: "id",
									},
								},
							},
						},
					},
					GroupByClause: []sql.ColumnReference{
						{ColumnName: "year"},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "year"},
				},
				"tbl2": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "year"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "year", TableID: "tbl2"},
				{Column: "count(*)"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"1", 1990}},
					{Vals: []interface{}{"2", 1991}},
					{Vals: []interface{}{"3", 1991}},
					{Vals: []interface{}{"4", 1992}},
					{Vals: []interface{}{"5", 1992}},
					{Vals: []interface{}{"6", 1992}},
					{Vals: []interface{}{"7", 1993}},
					{Vals: []interface{}{"8", 1993}},
					{Vals: []interface{}{"9", 1994}},
				},
				"tbl2": {
					{Vals: []interface{}{"1", 2000}},
					{Vals: []interface{}{"2", 2001}},
					{Vals: []interface{}{"3", 2001}},
					{Vals: []interface{}{"4", 2002}},
					{Vals: []interface{}{"5", 2002}},
					{Vals: []interface{}{"6", 2002}},
					{Vals: []interface{}{"7", 2003}},
					{Vals: []interface{}{"8", 2003}},
					{Vals: []interface{}{"9", 2004}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{2000, 1}},
				{Vals: []interface{}{2001, 2}},
				{Vals: []interface{}{2002, 3}},
				{Vals: []interface{}{2003, 2}},
				{Vals: []interface{}{2004, 1}},
			},
		},
		{
			name: `select aggregate with column alias: SELECT tbl2.year as yr, count(*)
		        FROM tbl1
		        JOIN tbl2 ON tbl1.id = tbl2.id
		        GROUP BY yr`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							Qualifier:  "tbl2",
							ColumnName: "year",
						},
						AsClause: "yr",
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Count{},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.QualifiedJoin{
							LHS:      sql.TableName{Name: "tbl1"},
							RHS:      sql.TableName{Name: "tbl2"},
							JoinType: sql.INNER_JOIN,
							JoinCondition: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS: sql.ColumnReference{
										Qualifier:  "tbl1",
										ColumnName: "id",
									},
									CompOp: sql.EQ,
									RHS: sql.ColumnReference{
										Qualifier:  "tbl2",
										ColumnName: "id",
									},
								},
							},
						},
					},
					GroupByClause: []sql.ColumnReference{
						{ColumnName: "yr"},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"tbl1": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "year"},
				},
				"tbl2": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "year"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "yr", TableID: "tbl2"},
				{Column: "count(*)"},
			},
			givenRows: map[string][]*storage.Row{
				"tbl1": {
					{Vals: []interface{}{"1", 1990}},
					{Vals: []interface{}{"2", 1991}},
					{Vals: []interface{}{"3", 1991}},
					{Vals: []interface{}{"4", 1992}},
					{Vals: []interface{}{"5", 1992}},
					{Vals: []interface{}{"6", 1992}},
					{Vals: []interface{}{"7", 1993}},
					{Vals: []interface{}{"8", 1993}},
					{Vals: []interface{}{"9", 1994}},
				},
				"tbl2": {
					{Vals: []interface{}{"1", 2000}},
					{Vals: []interface{}{"2", 2001}},
					{Vals: []interface{}{"3", 2001}},
					{Vals: []interface{}{"4", 2002}},
					{Vals: []interface{}{"5", 2002}},
					{Vals: []interface{}{"6", 2002}},
					{Vals: []interface{}{"7", 2003}},
					{Vals: []interface{}{"8", 2003}},
					{Vals: []interface{}{"9", 2004}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{2000, 1}},
				{Vals: []interface{}{2001, 2}},
				{Vals: []interface{}{2002, 3}},
				{Vals: []interface{}{2003, 2}},
				{Vals: []interface{}{2004, 1}},
			},
		},
		{
			name: "avg() using field qualifiers returns non-empty result set: SELECT avg(grades.math), avg(grades.science) as sci_avg FROM grades",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								Qualifier:  "grades",
								ColumnName: "math",
							},
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								Qualifier:  "grades",
								ColumnName: "science",
							},
						},
						AsClause: "sci_avg",
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "grades",
						},
					},
				},
			},
			givenRows: map[string][]*storage.Row{
				"grades": {
					{Vals: []interface{}{1, 100, 23}},
					{Vals: []interface{}{2, 74, 89}},
					{Vals: []interface{}{3, 34, 54}},
					{Vals: []interface{}{4, 90, 32}},
				},
			},
			givenFields: map[string]storage.Fields{
				"grades": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "math"},
					&storage.Field{Column: "science"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "avg(grades.math)"},
				{Column: "sci_avg"},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{74, 49}},
			},
		},
		{
			name: "avg() with implicit GROUP BY returns non-empty result set: SELECT avg(math), avg(science) FROM grades",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								ColumnName: "math",
							},
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								ColumnName: "science",
							},
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "grades",
						},
					},
				},
			},
			givenRows: map[string][]*storage.Row{
				"grades": {
					{Vals: []interface{}{1, 100, 23}},
					{Vals: []interface{}{2, 74, 89}},
					{Vals: []interface{}{3, 34, 54}},
					{Vals: []interface{}{4, 90, 32}},
				},
			},
			givenFields: map[string]storage.Fields{
				"grades": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "math"},
					&storage.Field{Column: "science"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "avg(math)"},
				{Column: "avg(science)"},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{74, 49}},
			},
		},
		{
			name: "avg() with implicit GROUP BY returns empty result set: SELECT avg(math), avg(science) FROM grades",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								ColumnName: "math",
							},
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								ColumnName: "science",
							},
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "grades",
						},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"grades": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "math"},
					&storage.Field{Column: "science"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "avg(math)"},
				{Column: "avg(science)"},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{0, 0}},
			},
		},
		{
			name: "avg() with GROUP BY returns non-empty result set: SELECT id, avg(math), avg(science) FROM grades group by id",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							ColumnName: "id",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								ColumnName: "math",
							},
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Average{
							ValueExpression: sql.ColumnReference{
								ColumnName: "science",
							},
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{
							Name: "grades",
						},
					},
					GroupByClause: []sql.ColumnReference{
						{ColumnName: "id"},
					},
				},
			},
			givenRows: map[string][]*storage.Row{
				"grades": {
					{Vals: []interface{}{4, 100, 25}},
					{Vals: []interface{}{1, 91, 75}},
					{Vals: []interface{}{2, 34, 87}},
					{Vals: []interface{}{3, 85, 12}},
					{Vals: []interface{}{1, 99, 25}},
					{Vals: []interface{}{3, 34, 63}},
					{Vals: []interface{}{4, 23, 72}},
					{Vals: []interface{}{2, 10, 39}},
				},
			},
			givenFields: map[string]storage.Fields{
				"grades": {
					&storage.Field{Column: "id"},
					&storage.Field{Column: "math"},
					&storage.Field{Column: "science"},
				},
			},
			expectFields: []*storage.Field{
				{TableID: "grades", Column: "id"},
				{Column: "avg(math)"},
				{Column: "avg(science)"},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{4, 62, 49}},
				{Vals: []interface{}{1, 95, 50}},
				{Vals: []interface{}{2, 22, 63}},
				{Vals: []interface{}{3, 60, 38}},
			},
		},
		{
			name: `select count(field): SELECT customer_id, count(product_id)
		        FROM orders
		        GROUP BY customer_id`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.ColumnReference{
							ColumnName: "customer_id",
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Count{
							ValueExpression: sql.ColumnReference{
								ColumnName: "product_id",
							},
						},
					},
				},
				TableExpression: sql.TableExpression{
					FromClause: sql.FromClause{
						sql.TableName{Name: "orders"},
					},
					GroupByClause: []sql.ColumnReference{
						{ColumnName: "customer_id"},
					},
				},
			},
			givenFields: map[string]storage.Fields{
				"orders": {
					&storage.Field{Column: "customer_id"},
					&storage.Field{Column: "product_id"},
				},
			},
			expectFields: []*storage.Field{
				{Column: "customer_id", TableID: "orders"},
				{Column: "count(product_id)"},
			},
			givenRows: map[string][]*storage.Row{
				"orders": {
					{Vals: []interface{}{"1", "A"}},
					{Vals: []interface{}{"1", "A"}},
					{Vals: []interface{}{"1", nil}},
					{Vals: []interface{}{"1", "B"}},
					{Vals: []interface{}{"1", nil}},
					{Vals: []interface{}{"2", "B"}},
					{Vals: []interface{}{"2", nil}},
					{Vals: []interface{}{"2", "C"}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"1", 3}},
				{Vals: []interface{}{"2", 2}},
			},
		},
	}

	for _, test := range tc {
		t.Run(test.name, func(t *testing.T) {
			actualRows, actualFields, err := EvaluateSelect(test.query, &mockRelationManager{
				fetch: func(tableName string) ([]*storage.Row, []*storage.Field, error) {
					return test.givenRows[tableName], test.givenFields[tableName], nil
				},
			})

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

func TestSelectComparisonOperators(t *testing.T) {
	buildSQL := func(compOp sql.TokenType, rhs any) sql.Select {
		return sql.Select{
			SelectList: sql.SelectList{
				sql.DerivedColumn{
					ValueExpressionPrimary: sql.Asterisk{},
				},
			},
			TableExpression: sql.TableExpression{
				FromClause: sql.FromClause{
					sql.TableName{
						Name: "tbl1",
					},
				},
				WhereClause: sql.WhereClause{
					SearchCondition: sql.Predicate{
						ComparisonPredicate: sql.ComparisonPredicate{
							LHS: sql.ColumnReference{
								ColumnName: "col1",
							},
							CompOp: compOp,
							RHS:    rhs,
						},
					},
				},
			},
		}
	}

	intRows := map[string][]*storage.Row{
		"tbl1": {
			{Vals: []interface{}{4}},
			{Vals: []interface{}{5}},
			{Vals: []interface{}{6}},
			{Vals: []interface{}{1}},
			{Vals: []interface{}{2}},
			{Vals: []interface{}{3}},
		},
	}
	strRows := map[string][]*storage.Row{
		"tbl1": {
			{Vals: []interface{}{"dog"}},
			{Vals: []interface{}{"egg"}},
			{Vals: []interface{}{"farm"}},
			{Vals: []interface{}{"apple"}},
			{Vals: []interface{}{"bail"}},
			{Vals: []interface{}{"cow"}},
		},
	}

	givenFields := map[string]storage.Fields{
		"tbl1": {
			&storage.Field{Column: "col1"},
		},
	}
	expectFields := []*storage.Field{
		{Column: "col1", TableID: "tbl1"},
	}

	tc := []struct {
		name         string
		query        sql.Select
		givenRows    map[string][]*storage.Row
		expectFields []*storage.Field
		expectRows   []*storage.Row
		expectErr    error
	}{
		{
			name:         "SELECT with > operator: SELECT * FROM tbl1 WHERE col1 > 4",
			query:        buildSQL(sql.GT, 4),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{5}},
				{Vals: []interface{}{6}},
			},
		},
		{
			name:         "SELECT with >= operator: SELECT * FROM tbl1 WHERE col1 >= 4",
			query:        buildSQL(sql.GTE, 4),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{4}},
				{Vals: []interface{}{5}},
				{Vals: []interface{}{6}},
			},
		},
		{
			name:         "SELECT with > operator: SELECT * FROM tbl1 WHERE col1 < 4",
			query:        buildSQL(sql.LT, 4),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{1}},
				{Vals: []interface{}{2}},
				{Vals: []interface{}{3}},
			},
		},
		{
			name:         "SELECT with >= operator: SELECT * FROM tbl1 WHERE col1 <= 4",
			query:        buildSQL(sql.LTE, 4),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{4}},
				{Vals: []interface{}{1}},
				{Vals: []interface{}{2}},
				{Vals: []interface{}{3}},
			},
		},
		{
			name:         "SELECT with > operator: SELECT * FROM tbl1 WHERE col1 > 'cow'",
			query:        buildSQL(sql.GT, "cow"),
			givenRows:    strRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{"dog"}},
				{Vals: []interface{}{"egg"}},
				{Vals: []interface{}{"farm"}},
			},
		},
		{
			name:         "SELECT with >= operator: SELECT * FROM tbl1 WHERE col1 >= 'cow'",
			query:        buildSQL(sql.GTE, "cow"),
			givenRows:    strRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{"dog"}},
				{Vals: []interface{}{"egg"}},
				{Vals: []interface{}{"farm"}},
				{Vals: []interface{}{"cow"}},
			},
		},
		{
			name:         "SELECT with < operator: SELECT * FROM tbl1 WHERE col1 < 'cow'",
			query:        buildSQL(sql.LT, "cow"),
			givenRows:    strRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{"apple"}},
				{Vals: []interface{}{"bail"}},
			},
		},
		{
			name:         "SELECT with <= operator: SELECT * FROM tbl1 WHERE col1 <= 'cow'",
			query:        buildSQL(sql.LTE, "cow"),
			givenRows:    strRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{"apple"}},
				{Vals: []interface{}{"bail"}},
				{Vals: []interface{}{"cow"}},
			},
		},

		{
			name:         "SELECT with > operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 > 4",
			query:        buildSQL(sql.GT, 4),
			givenRows:    strRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with >= operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 >= 4",
			query:        buildSQL(sql.GTE, 4),
			givenRows:    strRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with > operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 < 4",
			query:        buildSQL(sql.LT, 4),
			givenRows:    strRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with >= operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 <= 4",
			query:        buildSQL(sql.LTE, 4),
			givenRows:    strRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with > operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 > 'cow'",
			query:        buildSQL(sql.GT, "cow"),
			givenRows:    intRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with >= operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 >= 'cow'",
			query:        buildSQL(sql.GTE, "cow"),
			givenRows:    intRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with < operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 < 'cow'",
			query:        buildSQL(sql.LT, "cow"),
			givenRows:    intRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with <= operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 <= 'cow'",
			query:        buildSQL(sql.LTE, "cow"),
			givenRows:    intRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
	}

	for _, test := range tc {
		t.Run(test.name, func(t *testing.T) {
			actualRows, actualFields, err := EvaluateSelect(test.query, &mockRelationManager{
				fetch: func(tableName string) ([]*storage.Row, []*storage.Field, error) {
					return test.givenRows[tableName], givenFields[tableName], nil
				},
			})

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

func TestSelectScalar(t *testing.T) {
	givenFields := map[string]storage.Fields{
		"tbl1": {
			&storage.Field{Column: "col1"},
		},
	}
	query := sql.Select{
		SelectList: sql.SelectList{
			sql.DerivedColumn{
				ValueExpressionPrimary: 123,
			},
			sql.DerivedColumn{
				ValueExpressionPrimary: sql.ColumnReference{
					ColumnName: "col1",
				},
			},
			sql.DerivedColumn{
				ValueExpressionPrimary: "Test",
			},
		},
		TableExpression: sql.TableExpression{
			FromClause: sql.FromClause{
				sql.TableName{
					Name: "tbl1",
				},
			},
		},
	}
	givenRows := map[string][]*storage.Row{
		"tbl1": {
			{Vals: []interface{}{1}},
			{Vals: []interface{}{2}},
			{Vals: []interface{}{3}},
			{Vals: []interface{}{4}},
		},
	}
	expectFields := []*storage.Field{
		{Column: "?"},
		{TableID: "tbl1", Column: "col1"},
		{Column: "?"},
	}
	expectRows := []*storage.Row{
		{Vals: []interface{}{123, 1, "Test"}},
		{Vals: []interface{}{123, 2, "Test"}},
		{Vals: []interface{}{123, 3, "Test"}},
		{Vals: []interface{}{123, 4, "Test"}},
	}
	actualRows, actualFields, _ := EvaluateSelect(query, &mockRelationManager{
		fetch: func(tableName string) ([]*storage.Row, []*storage.Field, error) {
			return givenRows[tableName], givenFields[tableName], nil
		},
	})

	if !reflect.DeepEqual(expectRows, actualRows) {
		t.Fatalf("rows do not match. expected: %s actual: %s", expectRows, actualRows)
	}
	if !reflect.DeepEqual(expectFields, actualFields) {
		t.Fatalf("fields do not match. expected: %s actual: %s", expectFields, actualFields)
	}
}
