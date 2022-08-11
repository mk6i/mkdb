package sql

import (
	"fmt"
	"strings"
	"testing"
)

func TestScanSelect(t *testing.T) {

	const src = `SELECT field_1, field_2 FROM the_table WHERE ident = "some literal" OR ident2 = 12 OR ident != "a string"`

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
			Text: "some literal",
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
		{
			Type: OR,
		},
		{
			Type: IDENT,
			Text: "ident",
		},
		{
			Type: NEQ,
		},
		{
			Type: STR,
			Text: "a string",
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

func TestScanSelectJoin(t *testing.T) {

	const src = `SELECT table_1.field_1, table_2.field_2 FROM table_1 JOIN table_2 ON table_1.id = table_2.id`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
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

func TestScanSelectStar(t *testing.T) {

	const src = `SELECT * FROM the_table`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
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

func TestScanInsert(t *testing.T) {

	const src = `INSERT INTO thedatabase (column1, column2, column3) VALUES (1, "value2", "value3")`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
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

func TestScanUpdate(t *testing.T) {

	const src = `UPDATE thedatabase SET column1 = "val1", column2 = "val2", column3 = "val3" WHERE id = 4`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
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

func TestScanUse(t *testing.T) {

	const src = `USE thedatabase`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: USE,
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
