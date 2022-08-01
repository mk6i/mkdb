package sql

import (
	"fmt"
	"strings"
	"testing"
)

func TestScanSelect(t *testing.T) {

	const src = `SELECT field_1, field_2 FROM the_table WHERE ident = "some literal" OR ident2 = 12`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
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
		{
			Type: OR,
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
			Text: "12",
		},
	}

	for _, exp := range expected {
		if !ts.Next() {
			t.Error("ran out of tokens")
		}
		actual := ts.Cur()
		if exp.Type != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[exp.Type], Tokens[actual.Type]))
		}
		if exp.Text != actual.Text {
			t.Errorf(fmt.Sprintf("token text does not match. expected: %s actual: %s", exp.Text, actual.Text))
		}
	}
	if ts.Next() {
		t.Errorf("there are still tokens that remain in scanner. next: %s", ts.Cur().Text)
	}
}

func TestScanCreateTable(t *testing.T) {

	const src = `
		CREATE TABLE Persons (
			PersonID int,
			LastName varchar(255)
		);
	`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
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
			Type: IDENT,
			Text: "int",
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
		{
			Type: SEMICOLON,
		},
	}

	for _, exp := range expected {
		if !ts.Next() {
			t.Error("ran out of tokens")
		}
		actual := ts.Cur()
		if exp.Type != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[exp.Type], Tokens[actual.Type]))
		}
		if exp.Text != actual.Text {
			t.Errorf(fmt.Sprintf("token text does not match. expected: %s actual: %s", exp.Text, actual.Text))
		}
	}
	if ts.Next() {
		t.Errorf("there are still tokens that remain in scanner. next: %s", ts.Cur().Text)
	}
}

func TestScanCreateDatabase(t *testing.T) {

	const src = `CREATE DATABASE thedatabase`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
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

	for _, exp := range expected {
		if !ts.Next() {
			t.Error("ran out of tokens")
		}
		actual := ts.Cur()
		if exp.Type != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[exp.Type], Tokens[actual.Type]))
		}
		if exp.Text != actual.Text {
			t.Errorf(fmt.Sprintf("token text does not match. expected: %s actual: %s", exp.Text, actual.Text))
		}
	}
	if ts.Next() {
		t.Errorf("there are still tokens that remain in scanner. next: %s", ts.Cur().Text)
	}
}

func TestTokenList(t *testing.T) {
	tokens := []Token{
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
		{
			Type: OR,
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
			Text: "12",
		},
	}

	tl := TokenList{}
	for _, tok := range tokens {
		tl.Add(tok)
	}

	cur := 0

	for tl.Cur() != EOFToken {

		if cur > 0 {
			exp := tokens[cur-1]
			actual := tl.Prev()
			if exp.Type != actual.Type {
				t.Errorf(fmt.Sprintf("prev token type does not match. expected: %s actual: %s", Tokens[exp.Type], Tokens[actual.Type]))
			}
			if exp.Text != actual.Text {
				t.Errorf(fmt.Sprintf("prev token text does not match. expected: %s actual: %s", exp.Text, actual.Text))
			}
		}

		exp := tokens[cur]
		actual := tl.Cur()

		if exp.Type != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[exp.Type], Tokens[actual.Type]))
		}
		if exp.Text != actual.Text {
			t.Errorf(fmt.Sprintf("token text does not match. expected: %s actual: %s", exp.Text, actual.Text))
		}

		cur++

		if cur < len(tokens)-1 && !tl.HasNext() {
			t.Error("tokenList has more tokens than the input")
		}

		tl.Advance()
	}

	if cur != len(tokens) {
		t.Error("token list did not consume the same # of input tokens")
	}
}
