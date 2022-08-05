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
			Type: FROM,
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: WHERE,
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
				Token: Token{
					Type: IDENT,
					Text: "field_1",
				},
			},
			ValueExpression{
				Token: Token{
					Type: IDENT,
					Text: "field_2",
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				Relation("the_table"),
			},
			WhereClause: WhereClause{
				SearchCondition: BooleanTerm{
					lhs: Predicate{
						ComparisonPredicate{
							lhs: ValueExpression{
								Token{
									Type:   IDENT,
									Line:   0,
									Column: 0,
									Text:   "ident1",
								},
							},
							CompOp: EQ,
							rhs: ValueExpression{
								Token{
									Type:   STR,
									Line:   0,
									Column: 0,
									Text:   "\"some literal\"",
								},
							},
						},
					},
					rhs: Predicate{
						ComparisonPredicate{
							lhs: ValueExpression{
								Token{
									Type:   IDENT,
									Line:   0,
									Column: 0,
									Text:   "ident2",
								},
							},
							CompOp: NEQ,
							rhs: ValueExpression{
								Token{
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
				Token: Token{
					Type: ASTRSK,
				},
			},
		},
		TableExpression: TableExpression{
			FromClause: FromClause{
				Relation("the_table"),
			},
			WhereClause: WhereClause{
				SearchCondition: nil,
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
