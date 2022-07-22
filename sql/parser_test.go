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
			Text: "ident",
		},
		{
			Type: EQ,
		},
		{
			Type: STR,
			Text: "\"some literal\"",
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
		FromList: FromList{
			Relation("the_table"),
		},
		Condition: Condition{
			Left: Primary{
				Token: Token{
					Type: IDENT,
					Text: "ident",
				},
			},
			Right: Primary{
				Token: Token{
					Type: STR,
					Text: "\"some literal\"",
				},
			},
			Operator: EQ,
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
