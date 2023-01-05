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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
			},
			expectRows: []*storage.Row{
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
			name: "SELECT with ORDER BY, 2nd col has identical values: SELECT * FROM tbl1 ORDER BY col1 DESC, col2 ASC, col3 DESC",
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
							Name: "tbl1",
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
					{Vals: []interface{}{"a", int32(1), int32(4)}},
					{Vals: []interface{}{"a", int32(1), int32(5)}},
					{Vals: []interface{}{"a", int32(1), int32(6)}},
					{Vals: []interface{}{"b", int32(1), int32(1)}},
					{Vals: []interface{}{"b", int32(1), int32(2)}},
					{Vals: []interface{}{"b", int32(1), int32(3)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{"b", int32(1), int32(3)}},
				{Vals: []interface{}{"b", int32(1), int32(2)}},
				{Vals: []interface{}{"b", int32(1), int32(1)}},
				{Vals: []interface{}{"a", int32(1), int32(6)}},
				{Vals: []interface{}{"a", int32(1), int32(5)}},
				{Vals: []interface{}{"a", int32(1), int32(4)}},
			},
		},
		{
			name: "SELECT with ORDER BY on non-existent field: SELECT * FROM tbl1 ORDER BY non_existent_col DESC",
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
							Name: "tbl1",
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						ColumnName: sql.Token{
							Type: sql.ASTRSK,
						},
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
					sql.ValueExpression{
						Qualifier: sql.Token{
							Type: sql.IDENT,
							Text: "tbl1",
						},
						ColumnName: sql.Token{
							Type: sql.IDENT,
							Text: "col1",
						},
					},
					sql.ValueExpression{
						Qualifier: sql.Token{
							Type: sql.IDENT,
							Text: "tbl2",
						},
						ColumnName: sql.Token{
							Type: sql.IDENT,
							Text: "col3",
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
									LHS: sql.ValueExpression{
										Qualifier: sql.Token{
											Type: sql.IDENT,
											Text: "tbl1",
										},
										ColumnName: sql.Token{
											Type: sql.IDENT,
											Text: "id",
										},
									},
									CompOp: sql.EQ,
									RHS: sql.ValueExpression{
										Qualifier: sql.Token{
											Type: sql.IDENT,
											Text: "tbl2",
										},
										ColumnName: sql.Token{
											Type: sql.IDENT,
											Text: "id",
										},
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
					{Vals: []interface{}{"id_+1", int32(1), int32(2)}},
					{Vals: []interface{}{"id_+2", int32(3), int32(4)}},
					{Vals: []interface{}{"id_+5", int32(5), int32(6)}},
					{Vals: []interface{}{"id_+6", int32(7), int32(8)}},
					{Vals: []interface{}{"id_+7", int32(9), int32(10)}},
					{Vals: []interface{}{"id_+9", int32(11), int32(12)}},
					{Vals: []interface{}{"id_+10", int32(13), int32(14)}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_+1", int32(15), int32(16)}},
					{Vals: []interface{}{"id_-2", int32(17), int32(18)}},
					{Vals: []interface{}{"id_+5", int32(19), int32(20)}},
					{Vals: []interface{}{"id_-6", int32(21), int32(22)}},
					{Vals: []interface{}{"id_+7", int32(23), int32(24)}},
					{Vals: []interface{}{"id_-9", int32(25), int32(26)}},
					{Vals: []interface{}{"id_+10", int32(27), int32(28)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{int32(1), int32(15)}},
				{Vals: []interface{}{int32(5), int32(19)}},
				{Vals: []interface{}{int32(9), int32(23)}},
				{Vals: []interface{}{int32(13), int32(27)}},
			},
		},
		{
			name: `SELECT with LEFT JOIN: SELECT tbl1.col1, tbl2.col3 FROM tbl1 LEFT JOIN tbl2 ON tbl1.id = tbl2.id`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						Qualifier: sql.Token{
							Type: sql.IDENT,
							Text: "tbl1",
						},
						ColumnName: sql.Token{
							Type: sql.IDENT,
							Text: "col1",
						},
					},
					sql.ValueExpression{
						Qualifier: sql.Token{
							Type: sql.IDENT,
							Text: "tbl2",
						},
						ColumnName: sql.Token{
							Type: sql.IDENT,
							Text: "col3",
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
									LHS: sql.ValueExpression{
										Qualifier: sql.Token{
											Type: sql.IDENT,
											Text: "tbl1",
										},
										ColumnName: sql.Token{
											Type: sql.IDENT,
											Text: "id",
										},
									},
									CompOp: sql.EQ,
									RHS: sql.ValueExpression{
										Qualifier: sql.Token{
											Type: sql.IDENT,
											Text: "tbl2",
										},
										ColumnName: sql.Token{
											Type: sql.IDENT,
											Text: "id",
										},
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
					{Vals: []interface{}{"id_1", int32(1), int32(2)}},
					{Vals: []interface{}{"id_2", int32(3), int32(4)}},
					{Vals: []interface{}{"id_3", int32(5), int32(6)}},
					{Vals: []interface{}{"id_4", int32(7), int32(8)}},
					{Vals: []interface{}{"id_5", int32(9), int32(10)}},
					{Vals: []interface{}{"id_6", int32(11), int32(12)}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_1", int32(13), int32(14)}},
					{Vals: []interface{}{"id_3", int32(15), int32(16)}},
					{Vals: []interface{}{"id_5", int32(17), int32(18)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{int32(1), int32(13)}},
				{Vals: []interface{}{int32(3), nil}},
				{Vals: []interface{}{int32(5), int32(15)}},
				{Vals: []interface{}{int32(7), nil}},
				{Vals: []interface{}{int32(9), int32(17)}},
				{Vals: []interface{}{int32(11), nil}},
			},
		},
		{
			name: `SELECT with RIGHT JOIN: SELECT tbl1.col1, tbl2.col3 FROM tbl1 RIGHT JOIN tbl2 ON tbl1.id = tbl2.id`,
			query: sql.Select{
				SelectList: sql.SelectList{
					sql.ValueExpression{
						Qualifier: sql.Token{
							Type: sql.IDENT,
							Text: "tbl1",
						},
						ColumnName: sql.Token{
							Type: sql.IDENT,
							Text: "col1",
						},
					},
					sql.ValueExpression{
						Qualifier: sql.Token{
							Type: sql.IDENT,
							Text: "tbl2",
						},
						ColumnName: sql.Token{
							Type: sql.IDENT,
							Text: "col3",
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
									LHS: sql.ValueExpression{
										Qualifier: sql.Token{
											Type: sql.IDENT,
											Text: "tbl1",
										},
										ColumnName: sql.Token{
											Type: sql.IDENT,
											Text: "id",
										},
									},
									CompOp: sql.EQ,
									RHS: sql.ValueExpression{
										Qualifier: sql.Token{
											Type: sql.IDENT,
											Text: "tbl2",
										},
										ColumnName: sql.Token{
											Type: sql.IDENT,
											Text: "id",
										},
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
					{Vals: []interface{}{"id_2", int32(1), int32(2)}},
					{Vals: []interface{}{"id_4", int32(3), int32(4)}},
					{Vals: []interface{}{"id_6", int32(5), int32(6)}},
				},
				"tbl2": {
					{Vals: []interface{}{"id_1", int32(7), int32(8)}},
					{Vals: []interface{}{"id_2", int32(9), int32(10)}},
					{Vals: []interface{}{"id_3", int32(11), int32(12)}},
					{Vals: []interface{}{"id_4", int32(13), int32(14)}},
					{Vals: []interface{}{"id_5", int32(15), int32(16)}},
					{Vals: []interface{}{"id_6", int32(17), int32(18)}},
				},
			},
			expectRows: []*storage.Row{
				{Vals: []interface{}{nil, int32(7)}},
				{Vals: []interface{}{int32(1), int32(9)}},
				{Vals: []interface{}{nil, int32(11)}},
				{Vals: []interface{}{int32(3), int32(13)}},
				{Vals: []interface{}{nil, int32(15)}},
				{Vals: []interface{}{int32(5), int32(17)}},
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
