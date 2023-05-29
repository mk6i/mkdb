package sql

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseSelect(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: AS,
			Text: "AS",
		},
		{
			Type: IDENT,
			Text: "field_1_alias",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: IDENT,
			Text: "field_2_alias",
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: WHERE,
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "ident1",
		},
		{
			Type: EQ,
		},
		{
			Type: STR,
			Text: "\"some literal\"",
		},
		{
			Type: AND,
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "ident2",
		},
		{
			Type: NEQ,
		},
		{
			Type: INT,
			Text: "1234",
		},
		{
			Type: ORDER,
		},
		{
			Type: BY,
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: ASC,
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: DESC,
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "tt",
					ColumnName: "field_1",
				},
				AsClause: "field_1_alias",
			},
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "tt",
					ColumnName: "field_2",
				},
				AsClause: "field_2_alias",
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				TableName{
					CorrelationName: "tt",
					Name:            "the_table",
				},
			},
			WhereClause: WhereClause{
				SearchCondition: BooleanTerm{
					LHS: Predicate{
						ComparisonPredicate{
							LHS: ColumnReference{
								Qualifier:  "tt",
								ColumnName: "ident1",
							},
							CompOp: EQ,
							RHS:    "\"some literal\"",
						},
					},
					RHS: Predicate{
						ComparisonPredicate{
							LHS: ColumnReference{
								Qualifier:  "tt",
								ColumnName: "ident2",
							},
							CompOp: NEQ,
							RHS:    int64(1234),
						},
					},
				},
			},
		},
		SortSpecificationList: []SortSpecification{
			{
				SortKey: ColumnReference{
					Qualifier:  "tt",
					ColumnName: "field_1",
				},
				OrderingSpecification: Token{Type: ASC},
			},
			{
				SortKey: ColumnReference{
					Qualifier:  "tt",
					ColumnName: "field_2",
				},
				OrderingSpecification: Token{Type: DESC},
			},
			{
				SortKey: ColumnReference{
					Qualifier:  "tt",
					ColumnName: "field_1",
				},
				OrderingSpecification: Token{Type: ASC},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectLimitOffset(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: ASTRSK,
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: LIMIT,
		},
		{
			Type: INT,
			Text: "10",
		},
		{
			Type: OFFSET,
		},
		{
			Type: INT,
			Text: "20",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: Asterisk{},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				TableName{
					Name: "the_table",
				},
			},
		},
		LimitOffsetClause: LimitOffsetClause{
			LimitActive:  true,
			OffsetActive: true,
			Limit:        10,
			Offset:       20,
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectOffsetLimit(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: ASTRSK,
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: OFFSET,
		},
		{
			Type: INT,
			Text: "20",
		},
		{
			Type: LIMIT,
		},
		{
			Type: INT,
			Text: "10",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: Asterisk{},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				TableName{
					Name: "the_table",
				},
			},
		},
		LimitOffsetClause: LimitOffsetClause{
			LimitActive:  true,
			OffsetActive: true,
			Limit:        10,
			Offset:       20,
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectNegativeOffset(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: ASTRSK,
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: OFFSET,
		},
		{
			Type: INT,
			Text: "-20",
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	_, err := p.Parse()

	if !errors.Is(err, ErrNegativeOffset) {
		t.Errorf("expected error: %v got: %v", ErrNegativeOffset, err)
	}
}

func TestParseSelectNegativeLimit(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: ASTRSK,
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: LIMIT,
		},
		{
			Type: INT,
			Text: "-20",
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	_, err := p.Parse()

	if !errors.Is(err, ErrNegativeLimit) {
		t.Errorf("expected error: %v got: %v", ErrNegativeOffset, err)
	}
}

func TestParseSelectInnerJoinSansInnerKeyword(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_1",
					ColumnName: "field_1",
				},
			},
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_2",
					ColumnName: "field_2",
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				QualifiedJoin{
					LHS: QualifiedJoin{
						LHS:      TableName{Name: "table_1"},
						RHS:      TableName{Name: "table_2"},
						JoinType: INNER_JOIN,
						JoinCondition: Predicate{
							ComparisonPredicate{
								LHS: ColumnReference{
									Qualifier:  "table_1",
									ColumnName: "id",
								},
								CompOp: EQ,
								RHS: ColumnReference{
									Qualifier:  "table_2",
									ColumnName: "id",
								},
							},
						},
					},
					RHS:      TableName{Name: "table_3"},
					JoinType: INNER_JOIN,
					JoinCondition: Predicate{
						ComparisonPredicate{
							LHS: ColumnReference{
								Qualifier:  "table_2",
								ColumnName: "id",
							},
							CompOp: EQ,
							RHS: ColumnReference{
								Qualifier:  "table_3",
								ColumnName: "id",
							},
						},
					},
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectInnerJoinWithInnerKeyword(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: INNER,
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_1",
					ColumnName: "field_1",
				},
			},
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_2",
					ColumnName: "field_2",
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				QualifiedJoin{
					LHS: QualifiedJoin{
						LHS:      TableName{Name: "table_1"},
						RHS:      TableName{Name: "table_2"},
						JoinType: INNER_JOIN,
						JoinCondition: Predicate{
							ComparisonPredicate{
								LHS: ColumnReference{
									Qualifier:  "table_1",
									ColumnName: "id",
								},
								CompOp: EQ,
								RHS: ColumnReference{
									Qualifier:  "table_2",
									ColumnName: "id",
								},
							},
						},
					},
					RHS:      TableName{Name: "table_3"},
					JoinType: INNER_JOIN,
					JoinCondition: Predicate{
						ComparisonPredicate{
							LHS: ColumnReference{
								Qualifier:  "table_2",
								ColumnName: "id",
							},
							CompOp: EQ,
							RHS: ColumnReference{
								Qualifier:  "table_3",
								ColumnName: "id",
							},
						},
					},
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectLeftJoin(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: LEFT,
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_1",
					ColumnName: "field_1",
				},
			},
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_2",
					ColumnName: "field_2",
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				QualifiedJoin{
					LHS: QualifiedJoin{
						LHS:      TableName{Name: "table_1"},
						RHS:      TableName{Name: "table_2"},
						JoinType: LEFT_JOIN,
						JoinCondition: Predicate{
							ComparisonPredicate{
								LHS: ColumnReference{
									Qualifier:  "table_1",
									ColumnName: "id",
								},
								CompOp: EQ,
								RHS: ColumnReference{
									Qualifier:  "table_2",
									ColumnName: "id",
								},
							},
						},
					},
					RHS:      TableName{Name: "table_3"},
					JoinType: INNER_JOIN,
					JoinCondition: Predicate{
						ComparisonPredicate{
							LHS: ColumnReference{
								Qualifier:  "table_2",
								ColumnName: "id",
							},
							CompOp: EQ,
							RHS: ColumnReference{
								Qualifier:  "table_3",
								ColumnName: "id",
							},
						},
					},
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectRightJoin(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: RIGHT,
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: JOIN,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: ON,
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: IDENT,
			Text: "table_3",
		},
		{
			Type: DOT,
		},
		{
			Type: IDENT,
			Text: "id",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_1",
					ColumnName: "field_1",
				},
			},
			DerivedColumn{
				ValueExpressionPrimary: ColumnReference{
					Qualifier:  "table_2",
					ColumnName: "field_2",
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				QualifiedJoin{
					LHS: QualifiedJoin{
						LHS:      TableName{Name: "table_1"},
						RHS:      TableName{Name: "table_2"},
						JoinType: RIGHT_JOIN,
						JoinCondition: Predicate{
							ComparisonPredicate{
								LHS: ColumnReference{
									Qualifier:  "table_1",
									ColumnName: "id",
								},
								CompOp: EQ,
								RHS: ColumnReference{
									Qualifier:  "table_2",
									ColumnName: "id",
								},
							},
						},
					},
					RHS:      TableName{Name: "table_3"},
					JoinType: INNER_JOIN,
					JoinCondition: Predicate{
						ComparisonPredicate{
							LHS: ColumnReference{
								Qualifier:  "table_2",
								ColumnName: "id",
							},
							CompOp: EQ,
							RHS: ColumnReference{
								Qualifier:  "table_3",
								ColumnName: "id",
							},
						},
					},
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectStar(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: ASTRSK,
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: Asterisk{},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				TableName{Name: "the_table"},
			},
			WhereClause: nil,
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseCreateTable(t *testing.T) {

	input := []Token{
		{
			Type: CREATE,
		},
		{
			Type: TABLE,
		},
		{
			Type: IDENT,
			Text: "Persons",
		},
		{
			Type: LPAREN,
		},
		{
			Type: IDENT,
			Text: "PersonID",
		},
		{
			Type: T_INT,
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "Salary",
		},
		{
			Type: T_BIGINT,
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "LastName",
		},
		{
			Type: T_VARCHAR,
		},
		{
			Type: LPAREN,
		},
		{
			Type: INT,
			Text: "255",
		},
		{
			Type: RPAREN,
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "BoolVal",
		},
		{
			Type: T_BOOL,
		},
		{
			Type: RPAREN,
		},
	}

	expected := CreateTable{
		Name: "Persons",
		Elements: []TableElement{
			{
				ColumnDefinition{
					DataType: NumericType{},
					Name:     "PersonID",
				},
			},
			{
				ColumnDefinition{
					DataType: BigIntType{},
					Name:     "Salary",
				},
			},
			{
				ColumnDefinition{
					DataType: CharacterStringType{
						Len:  int64(255),
						Type: T_VARCHAR,
					},
					Name: "LastName",
				},
			},
			{
				ColumnDefinition{
					DataType: BooleanType{},
					Name:     "BoolVal",
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseCreateDatabase(t *testing.T) {

	input := []Token{
		{
			Type: CREATE,
		},
		{
			Type: DATABASE,
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
	}

	expected := CreateDatabase{
		Name: "thedatabase",
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseInsert(t *testing.T) {

	input := []Token{
		{
			Type: INSERT,
		},
		{
			Type: INTO,
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
		{
			Type: LPAREN,
		},
		{
			Type: IDENT,
			Text: "column1",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "column2",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "column3",
		},
		{
			Type: RPAREN,
		},
		{
			Type: VALUES,
		},
		{
			Type: LPAREN,
		},
		{
			Type: INT,
			Text: "1",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "2",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "3",
		},
		{
			Type: RPAREN,
		},
		{
			Type: COMMA,
		},
		{
			Type: LPAREN,
		},
		{
			Type: INT,
			Text: "4",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "5",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "6",
		},
		{
			Type: RPAREN,
		},
		{
			Type: COMMA,
		},
		{
			Type: LPAREN,
		},
		{
			Type: INT,
			Text: "7",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "8",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "9",
		},
		{
			Type: RPAREN,
		},
	}

	expected := InsertStatement{
		TableName: "thedatabase",
		InsertColumnsAndSource: InsertColumnsAndSource{
			InsertColumnList: InsertColumnList{
				ColumnNames: []string{
					"column1",
					"column2",
					"column3",
				},
			},
			QueryExpression: TableValueConstructor{
				TableValueConstructorList: []RowValueConstructor{
					{RowValueConstructorList: []interface{}{int64(1), "2", "3"}},
					{RowValueConstructorList: []interface{}{int64(4), "5", "6"}},
					{RowValueConstructorList: []interface{}{int64(7), "8", "9"}},
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseInsertSansColumnList(t *testing.T) {

	input := []Token{
		{
			Type: INSERT,
		},
		{
			Type: INTO,
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
		{
			Type: VALUES,
		},
		{
			Type: LPAREN,
		},
		{
			Type: INT,
			Text: "1",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "value2",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "value3",
		},
		{
			Type: COMMA,
		},
		{
			Type: TRUE,
		},
		{
			Type: RPAREN,
		},
	}

	expected := InsertStatement{
		TableName: "thedatabase",
		InsertColumnsAndSource: InsertColumnsAndSource{
			QueryExpression: TableValueConstructor{
				TableValueConstructorList: []RowValueConstructor{
					{RowValueConstructorList: []interface{}{int64(1), "value2", "value3", true}},
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseUpdate(t *testing.T) {

	input := []Token{
		{
			Type: UPDATE,
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
		{
			Type: SET,
		},
		{
			Type: IDENT,
			Text: "column1",
		},
		{
			Type: EQ,
		},
		{
			Type: STR,
			Text: "val1",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "column2",
		},
		{
			Type: EQ,
		},
		{
			Type: STR,
			Text: "val2",
		},
		{
			Type: COMMA,
		},
		{
			Type: IDENT,
			Text: "column3",
		},
		{
			Type: EQ,
		},
		{
			Type: STR,
			Text: "val3",
		},
		{
			Type: WHERE,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: INT,
			Text: "4",
		},
	}

	expected := UpdateStatementSearched{
		TableName: "thedatabase",
		Set: []SetClause{
			{
				ObjectColumn: "column1",
				UpdateSource: "val1",
			},
			{
				ObjectColumn: "column2",
				UpdateSource: "val2",
			},
			{
				ObjectColumn: "column3",
				UpdateSource: "val3",
			},
		},
		Where: WhereClause{
			SearchCondition: Predicate{
				ComparisonPredicate{
					LHS: ColumnReference{
						ColumnName: "id",
					},
					CompOp: EQ,
					RHS:    int64(4),
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseUse(t *testing.T) {

	input := []Token{
		{
			Type: USE,
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
	}

	expected := UseStatement{
		DBName: "thedatabase",
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseDelete(t *testing.T) {

	input := []Token{
		{
			Type: DELETE,
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "thetable",
		},
		{
			Type: WHERE,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: INT,
			Text: "4",
		},
	}

	expected := DeleteStatementSearched{
		TableName: "thetable",
		WhereClause: WhereClause{
			SearchCondition: Predicate{
				ComparisonPredicate{
					LHS: ColumnReference{
						ColumnName: "id",
					},
					CompOp: EQ,
					RHS:    int64(4),
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectCount(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: COUNT,
		},
		{
			Type: LPAREN,
		},
		{
			Type: ASTRSK,
		},
		{
			Type: RPAREN,
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: Count{},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				TableName{
					Name: "the_table",
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectScalar(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: INT,
			Text: "123",
		},
		{
			Type: COMMA,
		},
		{
			Type: STR,
			Text: "Test",
		},
		{
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: int64(123),
			},
			DerivedColumn{
				ValueExpressionPrimary: "Test",
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				TableName{
					Name: "the_table",
				},
			},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectAvg(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: AVG,
		},
		{
			Type: LPAREN,
		},
		{
			Type: IDENT,
			Text: "col1",
		},
		{
			Type: RPAREN,
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: Average{
					ValueExpression: ColumnReference{
						ColumnName: "col1",
					},
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: []TableReference{},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectBooleanExpressionWithoutFrom(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: INT,
			Text: "1",
		},
		{
			Type: EQ,
		},
		{
			Type: INT,
			Text: "2",
		},
	}

	expected := Select{
		SelectList: SelectList{
			DerivedColumn{
				ValueExpressionPrimary: Predicate{
					ComparisonPredicate{
						LHS:    int64(1),
						CompOp: EQ,
						RHS:    int64(2),
					},
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: []TableReference{},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %+v actual :%+v", expected, actual)
	}
}

func TestParseSelectExpressionWithMissingFrom(t *testing.T) {

	input := []Token{
		{
			Type: SELECT,
		},
		{
			Type: INT,
			Text: "1",
		},
		{
			Type: WHERE,
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
		},
		{
			Type: INT,
			Text: "4",
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{tl}

	_, err := p.Parse()

	if !errors.Is(err, ErrUnexpectedToken) {
		t.Errorf("expected ErrUnexpectedToken, got %v", err)
	}
}

func TestParseSelectCountGroupBy(t *testing.T) {
	tc := []struct {
		name      string
		input     []Token
		expect    Select
		expectErr error
	}{
		{
			name: "aggregate two fields on one table",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: COMMA,
				},
				{
					Type: IDENT,
					Text: "field_2",
				},
				{
					Type: COMMA,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: AS,
				},
				{
					Type: IDENT,
					Text: "field_2_alias",
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "the_table",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: IDENT,
					Text: "field_2",
				},
			},
			expect: Select{
				SelectList: SelectList{
					DerivedColumn{
						ValueExpressionPrimary: ColumnReference{
							ColumnName: "field_1",
						},
					},
					DerivedColumn{
						ValueExpressionPrimary: ColumnReference{
							ColumnName: "field_2",
						},
					},
					DerivedColumn{
						ValueExpressionPrimary: Count{},
						AsClause:               "field_2_alias",
					},
				},
				TableExpression: TableExpression{
					FromClause: FromClause{
						TableName{
							Name: "the_table",
						},
					},
					GroupByClause: []ColumnReference{
						{ColumnName: "field_1"},
						{ColumnName: "field_2"},
					},
				},
			},
		},
		{
			name: "aggregate aliased column with group by that references alias",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: AS,
				},
				{
					Type: IDENT,
					Text: "field_1_alias",
				},
				{
					Type: COMMA,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "the_table",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "field_1_alias",
				},
			},
			expect: Select{
				SelectList: SelectList{
					DerivedColumn{
						ValueExpressionPrimary: ColumnReference{
							ColumnName: "field_1",
						},
						AsClause: "field_1_alias",
					},
					DerivedColumn{
						ValueExpressionPrimary: Count{},
					},
				},
				TableExpression: TableExpression{
					FromClause: FromClause{
						TableName{
							Name: "the_table",
						},
					},
					GroupByClause: []ColumnReference{
						{ColumnName: "field_1_alias"},
					},
				},
			},
		},
		{
			name: "aggregate column with field qualifier on select and not on group by column",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: IDENT,
					Text: "tt",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: COMMA,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "the_table",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
			},
			expect: Select{
				SelectList: SelectList{
					DerivedColumn{
						ValueExpressionPrimary: ColumnReference{
							Qualifier:  "tt",
							ColumnName: "field_1",
						},
					},
					DerivedColumn{
						ValueExpressionPrimary: Count{},
					},
				},
				TableExpression: TableExpression{
					FromClause: FromClause{
						TableName{
							Name: "the_table",
						},
					},
					GroupByClause: []ColumnReference{
						{ColumnName: "field_1"},
					},
				},
			},
		},
		{
			name: "aggregate column with field qualifier on select and group by column",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: IDENT,
					Text: "tt",
					Line: 10,
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: COMMA,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "the_table",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "tt",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
			},
			expect: Select{
				SelectList: SelectList{
					DerivedColumn{
						ValueExpressionPrimary: ColumnReference{
							Qualifier:  "tt",
							ColumnName: "field_1",
						},
					},
					DerivedColumn{
						ValueExpressionPrimary: Count{},
					},
				},
				TableExpression: TableExpression{
					FromClause: FromClause{
						TableName{
							Name: "the_table",
						},
					},
					GroupByClause: []ColumnReference{
						{
							Qualifier:  "tt",
							ColumnName: "field_1",
						},
					},
				},
			},
		},
		{
			name: "aggregate joined tables",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: COMMA,
				},
				{
					Type: IDENT,
					Text: "table_1",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "common_field",
				},
				{
					Type: COMMA,
				},
				{
					Type: IDENT,
					Text: "table_2",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "common_field",
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "table_1",
				},
				{
					Type: JOIN,
				},
				{
					Type: IDENT,
					Text: "table_2",
				},
				{
					Type: ON,
				},
				{
					Type: IDENT,
					Text: "table_1",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "id",
				},
				{
					Type: EQ,
				},
				{
					Type: IDENT,
					Text: "table_2",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "id",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "table_1",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "common_field",
				},
			},
			expectErr: ErrInvalidGroupByColumn,
		},
		{
			name: "aggregation with ambiguous group by that could match 2 select columns",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: COMMA,
				},
				{
					Type: IDENT,
					Text: "table_1",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "common_field",
				},
				{
					Type: COMMA,
				},
				{
					Type: IDENT,
					Text: "table_2",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "common_field",
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "table_1",
				},
				{
					Type: JOIN,
				},
				{
					Type: IDENT,
					Text: "table_2",
				},
				{
					Type: ON,
				},
				{
					Type: IDENT,
					Text: "table_1",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "id",
				},
				{
					Type: EQ,
				},
				{
					Type: IDENT,
					Text: "table_2",
				},
				{
					Type: DOT,
				},
				{
					Type: IDENT,
					Text: "id",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "common_field",
				},
			},
			expectErr: ErrAmbiguousGroupByColumn,
		},
		{
			name: "implicit aggregation with non-aggregated select column",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: COMMA,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "the_table",
				},
			},
			expectErr: ErrInvalidGroupByColumn,
		},
		{
			name: "explicit aggregation with non-aggregated select column",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: COMMA,
				},
				{
					Type: IDENT,
					Text: "field_2",
				},
				{
					Type: COMMA,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: ASTRSK,
				},
				{
					Type: RPAREN,
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "the_table",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
			},
			expectErr: ErrInvalidGroupByColumn,
		},
		{
			name: "count(field_name)",
			input: []Token{
				{
					Type: SELECT,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
				{
					Type: COMMA,
				},
				{
					Type: COUNT,
				},
				{
					Type: LPAREN,
				},
				{
					Type: IDENT,
					Text: "field_2",
				},
				{
					Type: RPAREN,
				},
				{
					Type: FROM,
				},
				{
					Type: IDENT,
					Text: "the_table",
				},
				{
					Type: GROUP,
				},
				{
					Type: BY,
				},
				{
					Type: IDENT,
					Text: "field_1",
				},
			},
			expect: Select{
				SelectList: SelectList{
					DerivedColumn{
						ValueExpressionPrimary: ColumnReference{
							ColumnName: "field_1",
						},
					},
					DerivedColumn{
						ValueExpressionPrimary: Count{
							ValueExpression: ColumnReference{
								ColumnName: "field_2",
							},
						},
					},
				},
				TableExpression: TableExpression{
					FromClause: FromClause{
						TableName{
							Name: "the_table",
						},
					},
					GroupByClause: []ColumnReference{
						{ColumnName: "field_1"},
					},
				},
			},
		},
	}

	for _, test := range tc {
		t.Run(test.name, func(t *testing.T) {
			tl := TokenList{
				tokens: test.input,
				cur:    0,
			}
			p := &Parser{tl}

			actual, err := p.Parse()

			if !errors.Is(err, test.expectErr) {
				t.Errorf("expected %v, got %v", test.expectErr, err)
			}
			if test.expectErr == nil && !reflect.DeepEqual(test.expect, actual) {
				t.Errorf("ASTs are not the same. expected: %+v actual :%+v", test.expect, actual)
			}
		})
	}
}
