package sql

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {

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
			Type: EQ,
		},
		{
			Type: INT,
			Text: "1234",
		},
	}

	expected := Select{
		SelectList: SelectList{
			Primary{
				Token: Token{
					Type: IDENT,
					Text: "field_1",
				},
			},
			Primary{
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
			WhereClause: &WhereClause{},
		},
	}

	tl := TokenList{
		tokens: input,
		cur:    0,
	}
	p := &Parser{&tl}

	actual, err := p.Parse()

	if err != nil {
		t.Errorf("parsing failed: %s", err.Error())
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ASTs are not the same. expected: %v actual :%v", expected, actual)
	}
}
