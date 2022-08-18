package sql

import (
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
	}

	expected := Select{
		SelectList: SelectList{
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "tt",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_1",
				},
			},
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "tt",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_2",
				},
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
							LHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "tt",
								},
								ColumnName: Token{
									Type:   IDENT,
									Line:   0,
									Column: 0,
									Text:   "ident1",
								},
							},
							CompOp: EQ,
							RHS: ValueExpression{
								ColumnName: Token{
									Type:   STR,
									Line:   0,
									Column: 0,
									Text:   "\"some literal\"",
								},
							},
						},
					},
					RHS: Predicate{
						ComparisonPredicate{
							LHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "tt",
								},
								ColumnName: Token{
									Type:   IDENT,
									Line:   0,
									Column: 0,
									Text:   "ident2",
								},
							},
							CompOp: NEQ,
							RHS: ValueExpression{
								ColumnName: Token{
									Type:   INT,
									Line:   0,
									Column: 0,
									Text:   "1234",
								},
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

func TestParseSelectJoin(t *testing.T) {

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
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "table_1",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_1",
				},
			},
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "table_2",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_2",
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				QualifiedJoin{
					LHS: QualifiedJoin{
						LHS:      TableName{Name: "table_1"},
						RHS:      TableName{Name: "table_2"},
						JoinType: REGULAR_JOIN,
						JoinCondition: Predicate{
							ComparisonPredicate{
								LHS: ValueExpression{
									Qualifier: Token{
										Type: IDENT,
										Text: "table_1",
									},
									ColumnName: Token{
										Type: IDENT,
										Text: "id",
									},
								},
								CompOp: EQ,
								RHS: ValueExpression{
									Qualifier: Token{
										Type: IDENT,
										Text: "table_2",
									},
									ColumnName: Token{
										Type: IDENT,
										Text: "id",
									},
								},
							},
						},
					},
					RHS:      TableName{Name: "table_3"},
					JoinType: REGULAR_JOIN,
					JoinCondition: Predicate{
						ComparisonPredicate{
							LHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "table_2",
								},
								ColumnName: Token{
									Type: IDENT,
									Text: "id",
								},
							},
							CompOp: EQ,
							RHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "table_3",
								},
								ColumnName: Token{
									Type: IDENT,
									Text: "id",
								},
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
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "table_1",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_1",
				},
			},
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "table_2",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_2",
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
								LHS: ValueExpression{
									Qualifier: Token{
										Type: IDENT,
										Text: "table_1",
									},
									ColumnName: Token{
										Type: IDENT,
										Text: "id",
									},
								},
								CompOp: EQ,
								RHS: ValueExpression{
									Qualifier: Token{
										Type: IDENT,
										Text: "table_2",
									},
									ColumnName: Token{
										Type: IDENT,
										Text: "id",
									},
								},
							},
						},
					},
					RHS:      TableName{Name: "table_3"},
					JoinType: REGULAR_JOIN,
					JoinCondition: Predicate{
						ComparisonPredicate{
							LHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "table_2",
								},
								ColumnName: Token{
									Type: IDENT,
									Text: "id",
								},
							},
							CompOp: EQ,
							RHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "table_3",
								},
								ColumnName: Token{
									Type: IDENT,
									Text: "id",
								},
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
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "table_1",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_1",
				},
			},
			ValueExpression{
				Qualifier: Token{
					Type: IDENT,
					Text: "table_2",
				},
				ColumnName: Token{
					Type: IDENT,
					Text: "field_2",
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
								LHS: ValueExpression{
									Qualifier: Token{
										Type: IDENT,
										Text: "table_1",
									},
									ColumnName: Token{
										Type: IDENT,
										Text: "id",
									},
								},
								CompOp: EQ,
								RHS: ValueExpression{
									Qualifier: Token{
										Type: IDENT,
										Text: "table_2",
									},
									ColumnName: Token{
										Type: IDENT,
										Text: "id",
									},
								},
							},
						},
					},
					RHS:      TableName{Name: "table_3"},
					JoinType: REGULAR_JOIN,
					JoinCondition: Predicate{
						ComparisonPredicate{
							LHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "table_2",
								},
								ColumnName: Token{
									Type: IDENT,
									Text: "id",
								},
							},
							CompOp: EQ,
							RHS: ValueExpression{
								Qualifier: Token{
									Type: IDENT,
									Text: "table_3",
								},
								ColumnName: Token{
									Type: IDENT,
									Text: "id",
								},
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
			ValueExpression{
				ColumnName: Token{
					Type: ASTRSK,
				},
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
					DataType: CharacterStringType{
						Len:  255,
						Type: T_VARCHAR,
					},
					Name: "LastName",
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
			TableValueConstructor: TableValueConstructor{
				Columns: []interface{}{
					int32(1),
					"value2",
					"value3",
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
			Type: RPAREN,
		},
	}

	expected := InsertStatement{
		TableName: "thedatabase",
		InsertColumnsAndSource: InsertColumnsAndSource{
			TableValueConstructor: TableValueConstructor{
				Columns: []interface{}{
					int32(1),
					"value2",
					"value3",
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
				UpdateSource: ValueExpression{
					ColumnName: Token{
						Type: STR,
						Text: "val1",
					},
				},
			},
			{
				ObjectColumn: "column2",
				UpdateSource: ValueExpression{
					ColumnName: Token{
						Type: STR,
						Text: "val2",
					},
				},
			},
			{
				ObjectColumn: "column3",
				UpdateSource: ValueExpression{
					ColumnName: Token{
						Type: STR,
						Text: "val3",
					},
				},
			},
		},
		Where: WhereClause{
			SearchCondition: Predicate{
				ComparisonPredicate{
					LHS: ValueExpression{
						ColumnName: Token{
							Type:   IDENT,
							Line:   0,
							Column: 0,
							Text:   "id",
						},
					},
					CompOp: EQ,
					RHS: ValueExpression{
						ColumnName: Token{
							Type:   INT,
							Line:   0,
							Column: 0,
							Text:   "4",
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
