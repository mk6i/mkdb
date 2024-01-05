package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mk6i/mkdb/sql"
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
					{Vals: []interface{}{"c", int64(1), int64(8)}},
					{Vals: []interface{}{"c", int64(5), int64(4)}},
					{Vals: []interface{}{"c", int64(5), int64(5)}},
					{Vals: []interface{}{"b", int64(5), int64(5)}},
					{Vals: []interface{}{"b", int64(6), int64(6)}},
					{Vals: []interface{}{"b", int64(7), int64(7)}},
					{Vals: []interface{}{"a", int64(2), int64(2)}},
					{Vals: []interface{}{"a", int64(3), int64(3)}},
					{Vals: []interface{}{"a", int64(5), int64(5)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"c", int64(1), int64(8)}},
				{Vals: []interface{}{"c", int64(5), int64(5)}},
				{Vals: []interface{}{"c", int64(5), int64(4)}},
				{Vals: []interface{}{"b", int64(5), int64(5)}},
				{Vals: []interface{}{"b", int64(6), int64(6)}},
				{Vals: []interface{}{"b", int64(7), int64(7)}},
				{Vals: []interface{}{"a", int64(2), int64(2)}},
				{Vals: []interface{}{"a", int64(3), int64(3)}},
				{Vals: []interface{}{"a", int64(5), int64(5)}},
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
					{Vals: []interface{}{"a", int64(1), int64(4)}},
					{Vals: []interface{}{"a", int64(1), int64(5)}},
					{Vals: []interface{}{"a", int64(1), int64(6)}},
					{Vals: []interface{}{"b", int64(1), int64(1)}},
					{Vals: []interface{}{"b", int64(1), int64(2)}},
					{Vals: []interface{}{"b", int64(1), int64(3)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"b", int64(1), int64(3)}},
				{Vals: []interface{}{"b", int64(1), int64(2)}},
				{Vals: []interface{}{"b", int64(1), int64(1)}},
				{Vals: []interface{}{"a", int64(1), int64(6)}},
				{Vals: []interface{}{"a", int64(1), int64(5)}},
				{Vals: []interface{}{"a", int64(1), int64(4)}},
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
					{Vals: []interface{}{"id_+1", int64(1), int64(2)}},
					{Vals: []interface{}{"id_+2", int64(3), int64(4)}},
					{Vals: []interface{}{"id_+5", int64(5), int64(6)}},
					{Vals: []interface{}{"id_+6", int64(7), int64(8)}},
					{Vals: []interface{}{"id_+7", int64(9), int64(10)}},
					{Vals: []interface{}{"id_+9", int64(11), int64(12)}},
					{Vals: []interface{}{"id_+10", int64(13), int64(14)}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_+1", int64(15), int64(16)}},
					{Vals: []interface{}{"id_-2", int64(17), int64(18)}},
					{Vals: []interface{}{"id_+5", int64(19), int64(20)}},
					{Vals: []interface{}{"id_-6", int64(21), int64(22)}},
					{Vals: []interface{}{"id_+7", int64(23), int64(24)}},
					{Vals: []interface{}{"id_-9", int64(25), int64(26)}},
					{Vals: []interface{}{"id_+10", int64(27), int64(28)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(1), int64(15)}},
				{Vals: []interface{}{int64(5), int64(19)}},
				{Vals: []interface{}{int64(9), int64(23)}},
				{Vals: []interface{}{int64(13), int64(27)}},
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
					{Vals: []interface{}{"id_1", int64(1), int64(2)}},
					{Vals: []interface{}{"id_2", int64(3), int64(4)}},
					{Vals: []interface{}{"id_3", int64(5), int64(6)}},
					{Vals: []interface{}{"id_4", int64(7), int64(8)}},
					{Vals: []interface{}{"id_5", int64(9), int64(10)}},
					{Vals: []interface{}{"id_6", int64(11), int64(12)}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_1", int64(13), int64(14)}},
					{Vals: []interface{}{"id_3", int64(15), int64(16)}},
					{Vals: []interface{}{"id_5", int64(17), int64(18)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(1), int64(13)}},
				{Vals: []interface{}{int64(3), nil}},
				{Vals: []interface{}{int64(5), int64(15)}},
				{Vals: []interface{}{int64(7), nil}},
				{Vals: []interface{}{int64(9), int64(17)}},
				{Vals: []interface{}{int64(11), nil}},
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
					{Vals: []interface{}{"id_2", int64(1), int64(2)}},
					{Vals: []interface{}{"id_4", int64(3), int64(4)}},
					{Vals: []interface{}{"id_6", int64(5), int64(6)}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_1", int64(7), int64(8)}},
					{Vals: []interface{}{"id_2", int64(9), int64(10)}},
					{Vals: []interface{}{"id_3", int64(11), int64(12)}},
					{Vals: []interface{}{"id_4", int64(13), int64(14)}},
					{Vals: []interface{}{"id_5", int64(15), int64(16)}},
					{Vals: []interface{}{"id_6", int64(17), int64(18)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{nil, int64(7)}},
				{Vals: []interface{}{int64(1), int64(9)}},
				{Vals: []interface{}{nil, int64(11)}},
				{Vals: []interface{}{int64(3), int64(13)}},
				{Vals: []interface{}{nil, int64(15)}},
				{Vals: []interface{}{int64(5), int64(17)}},
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
								LHS:    int64(1),
								CompOp: sql.EQ,
								RHS:    int64(1),
							},
						},
					},
					sql.DerivedColumn{
						ValueExpressionPrimary: sql.Predicate{
							ComparisonPredicate: sql.ComparisonPredicate{
								LHS:    int64(1),
								CompOp: sql.EQ,
								RHS:    int64(2),
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
									LHS:    int64(1),
									CompOp: sql.EQ,
									RHS:    int64(1),
								},
							},
							RHS: sql.Predicate{
								ComparisonPredicate: sql.ComparisonPredicate{
									LHS:    int64(2),
									CompOp: sql.EQ,
									RHS:    int64(2),
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
				{Vals: []interface{}{"1", "A", int64(2)}},
				{Vals: []interface{}{"1", "B", int64(1)}},
				{Vals: []interface{}{"2", "B", int64(2)}},
				{Vals: []interface{}{"2", "C", int64(1)}},
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
					{Vals: []interface{}{int64(1)}},
					{Vals: []interface{}{int64(2)}},
					{Vals: []interface{}{int64(3)}},
					{Vals: []interface{}{int64(4)}},
				},
			},
			expectFields: []*storage.Field{
				{Column: "count(*)"},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(4)}},
			},
		},
		{
			name: "implicit aggregation count(*) and scalar expression with empty result set: SELECT 1, count(*) FROM tbl1",
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.DerivedColumn{
						ValueExpressionPrimary: int64(1),
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
				{Vals: []interface{}{int64(1), int64(0)}},
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
					{Vals: []interface{}{"1", int64(1990)}},
					{Vals: []interface{}{"2", int64(1991)}},
					{Vals: []interface{}{"3", int64(1991)}},
					{Vals: []interface{}{"4", int64(1992)}},
					{Vals: []interface{}{"5", int64(1992)}},
					{Vals: []interface{}{"6", int64(1992)}},
					{Vals: []interface{}{"7", int64(1993)}},
					{Vals: []interface{}{"8", int64(1993)}},
					{Vals: []interface{}{"9", int64(1994)}},
				},
				"tbl2": {
					{Vals: []interface{}{"1", int64(2000)}},
					{Vals: []interface{}{"2", int64(2001)}},
					{Vals: []interface{}{"3", int64(2001)}},
					{Vals: []interface{}{"4", int64(2002)}},
					{Vals: []interface{}{"5", int64(2002)}},
					{Vals: []interface{}{"6", int64(2002)}},
					{Vals: []interface{}{"7", int64(2003)}},
					{Vals: []interface{}{"8", int64(2003)}},
					{Vals: []interface{}{"9", int64(2004)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(2000), int64(1)}},
				{Vals: []interface{}{int64(2001), int64(2)}},
				{Vals: []interface{}{int64(2002), int64(3)}},
				{Vals: []interface{}{int64(2003), int64(2)}},
				{Vals: []interface{}{int64(2004), int64(1)}},
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
					{Vals: []interface{}{"1", int64(1990)}},
					{Vals: []interface{}{"2", int64(1991)}},
					{Vals: []interface{}{"3", int64(1991)}},
					{Vals: []interface{}{"4", int64(1992)}},
					{Vals: []interface{}{"5", int64(1992)}},
					{Vals: []interface{}{"6", int64(1992)}},
					{Vals: []interface{}{"7", int64(1993)}},
					{Vals: []interface{}{"8", int64(1993)}},
					{Vals: []interface{}{"9", int64(1994)}},
				},
				"tbl2": {
					{Vals: []interface{}{"1", int64(2000)}},
					{Vals: []interface{}{"2", int64(2001)}},
					{Vals: []interface{}{"3", int64(2001)}},
					{Vals: []interface{}{"4", int64(2002)}},
					{Vals: []interface{}{"5", int64(2002)}},
					{Vals: []interface{}{"6", int64(2002)}},
					{Vals: []interface{}{"7", int64(2003)}},
					{Vals: []interface{}{"8", int64(2003)}},
					{Vals: []interface{}{"9", int64(2004)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(2000), int64(1)}},
				{Vals: []interface{}{int64(2001), int64(2)}},
				{Vals: []interface{}{int64(2002), int64(3)}},
				{Vals: []interface{}{int64(2003), int64(2)}},
				{Vals: []interface{}{int64(2004), int64(1)}},
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
					{Vals: []interface{}{int64(1), int64(100), int64(23)}},
					{Vals: []interface{}{int64(2), int64(74), int64(89)}},
					{Vals: []interface{}{int64(3), int64(34), int64(54)}},
					{Vals: []interface{}{int64(4), int64(90), int64(32)}},
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
				{Vals: []interface{}{int64(74), int64(49)}},
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
					{Vals: []interface{}{int64(1), int64(100), int64(23)}},
					{Vals: []interface{}{int64(2), int64(74), int64(89)}},
					{Vals: []interface{}{int64(3), int64(34), int64(54)}},
					{Vals: []interface{}{int64(4), int64(90), int64(32)}},
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
				{Vals: []interface{}{int64(74), int64(49)}},
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
				{Vals: []interface{}{int64(0), int64(0)}},
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
					{Vals: []interface{}{int64(4), int64(100), int64(25)}},
					{Vals: []interface{}{int64(1), int64(91), int64(75)}},
					{Vals: []interface{}{int64(2), int64(34), int64(87)}},
					{Vals: []interface{}{int64(3), int64(85), int64(12)}},
					{Vals: []interface{}{int64(1), int64(99), int64(25)}},
					{Vals: []interface{}{int64(3), int64(34), int64(63)}},
					{Vals: []interface{}{int64(4), int64(23), int64(72)}},
					{Vals: []interface{}{int64(2), int64(10), int64(39)}},
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
				{Vals: []interface{}{int64(4), int64(62), int64(49)}},
				{Vals: []interface{}{int64(1), int64(95), int64(50)}},
				{Vals: []interface{}{int64(2), int64(22), int64(63)}},
				{Vals: []interface{}{int64(3), int64(60), int64(38)}},
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
				{Vals: []interface{}{"1", int64(3)}},
				{Vals: []interface{}{"2", int64(2)}},
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
			{Vals: []interface{}{int64(4)}},
			{Vals: []interface{}{int64(5)}},
			{Vals: []interface{}{int64(6)}},
			{Vals: []interface{}{int64(1)}},
			{Vals: []interface{}{int64(2)}},
			{Vals: []interface{}{int64(3)}},
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
			query:        buildSQL(sql.GT, int64(4)),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(5)}},
				{Vals: []interface{}{int64(6)}},
			},
		},
		{
			name:         "SELECT with >= operator: SELECT * FROM tbl1 WHERE col1 >= 4",
			query:        buildSQL(sql.GTE, int64(4)),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(4)}},
				{Vals: []interface{}{int64(5)}},
				{Vals: []interface{}{int64(6)}},
			},
		},
		{
			name:         "SELECT with > operator: SELECT * FROM tbl1 WHERE col1 < 4",
			query:        buildSQL(sql.LT, int64(4)),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(1)}},
				{Vals: []interface{}{int64(2)}},
				{Vals: []interface{}{int64(3)}},
			},
		},
		{
			name:         "SELECT with >= operator: SELECT * FROM tbl1 WHERE col1 <= 4",
			query:        buildSQL(sql.LTE, int64(4)),
			givenRows:    intRows,
			expectFields: expectFields,
			expectRows: []*storage.Row{
				{Vals: []interface{}{int64(4)}},
				{Vals: []interface{}{int64(1)}},
				{Vals: []interface{}{int64(2)}},
				{Vals: []interface{}{int64(3)}},
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
			query:        buildSQL(sql.GT, int64(4)),
			givenRows:    strRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with >= operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 >= 4",
			query:        buildSQL(sql.GTE, int64(4)),
			givenRows:    strRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with > operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 < 4",
			query:        buildSQL(sql.LT, int64(4)),
			givenRows:    strRows,
			expectFields: nil,
			expectErr:    ErrIncompatTypeCompare,
		},
		{
			name:         "SELECT with >= operator and mismatched data types: SELECT * FROM tbl1 WHERE col1 <= 4",
			query:        buildSQL(sql.LTE, int64(4)),
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
				ValueExpressionPrimary: int64(123),
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
			{Vals: []interface{}{int64(1)}},
			{Vals: []interface{}{int64(2)}},
			{Vals: []interface{}{int64(3)}},
			{Vals: []interface{}{int64(4)}},
		},
	}
	expectFields := []*storage.Field{
		{Column: "?"},
		{TableID: "tbl1", Column: "col1"},
		{Column: "?"},
	}
	expectRows := []*storage.Row{
		{Vals: []interface{}{int64(123), int64(1), "Test"}},
		{Vals: []interface{}{int64(123), int64(2), "Test"}},
		{Vals: []interface{}{int64(123), int64(3), "Test"}},
		{Vals: []interface{}{int64(123), int64(4), "Test"}},
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
