package sql

import (
	"fmt"
	"strings"
	"testing"
)

func TestScanSelect(t *testing.T) {

	const src = `
		SELECT "tt"."field_1" AS field_1_alias, tt.field_2
		FROM the_table tt
		WHERE tt.ident = 'some literal'
			OR tt.ident2 = 12
			OR tt.ident != 'a string'
		ORDER BY tt.field_1 ASC, tt.field_2 DESC
		LIMIT 10 OFFSET 20
	`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: SELECT,
			Text: "SELECT",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
			Text: ".",
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
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
			Text: "FROM",
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
			Text: "WHERE",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "ident",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: STR,
			Text: "some literal",
		},
		{
			Type: OR,
			Text: "OR",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "ident2",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: INT,
			Text: "12",
		},
		{
			Type: OR,
			Text: "OR",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "ident",
		},
		{
			Type: NEQ,
			Text: "!",
		},
		{
			Type: STR,
			Text: "a string",
		},
		{
			Type: ORDER,
			Text: "ORDER",
		},
		{
			Type: BY,
			Text: "BY",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: ASC,
			Text: "ASC",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "tt",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: DESC,
			Text: "DESC",
		},
		{
			Type: LIMIT,
			Text: "LIMIT",
		},
		{
			Type: INT,
			Text: "10",
		},
		{
			Type: OFFSET,
			Text: "OFFSET",
		},
		{
			Type: INT,
			Text: "20",
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

func TestScanSelectInnerJoin(t *testing.T) {

	const src = `SELECT table_1.field_1, table_2.field_2 FROM table_1 INNER JOIN table_2 ON table_1.id = table_2.id`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: SELECT,
			Text: "SELECT",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
			Text: "FROM",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: INNER,
			Text: "INNER",
		},
		{
			Type: JOIN,
			Text: "JOIN",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: ON,
			Text: "ON",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
			Text: ".",
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

func TestScanSelectLeftJoin(t *testing.T) {

	const src = `SELECT table_1.field_1, table_2.field_2 FROM table_1 LEFT JOIN table_2 ON table_1.id = table_2.id`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: SELECT,
			Text: "SELECT",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
			Text: "FROM",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: LEFT,
			Text: "LEFT",
		},
		{
			Type: JOIN,
			Text: "JOIN",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: ON,
			Text: "ON",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
			Text: ".",
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

func TestScanSelectRightJoin(t *testing.T) {

	const src = `SELECT table_1.field_1, table_2.field_2 FROM table_1 RIGHT JOIN table_2 ON table_1.id = table_2.id`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: SELECT,
			Text: "SELECT",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
			Text: "FROM",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: RIGHT,
			Text: "RIGHT",
		},
		{
			Type: JOIN,
			Text: "JOIN",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: ON,
			Text: "ON",
		},
		{
			Type: IDENT,
			Text: "table_1",
		},
		{
			Type: DOT,
			Text: ".",
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: IDENT,
			Text: "table_2",
		},
		{
			Type: DOT,
			Text: ".",
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
			Text: "SELECT",
		},
		{
			Type: ASTRSK,
			Text: "*",
		},
		{
			Type: FROM,
			Text: "FROM",
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
			LastName varchar(255),
			BoolValue boolean,
			Salary bigint
		);
	`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: CREATE,
			Text: "CREATE",
		},
		{
			Type: TABLE,
			Text: "TABLE",
		},
		{
			Type: IDENT,
			Text: "Persons",
		},
		{
			Type: LPAREN,
			Text: "(",
		},
		{
			Type: IDENT,
			Text: "PersonID",
		},
		{
			Type: T_INT,
			Text: "int",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "LastName",
		},
		{
			Type: T_VARCHAR,
			Text: "varchar",
		},
		{
			Type: LPAREN,
			Text: "(",
		},
		{
			Type: INT,
			Text: "255",
		},
		{
			Type: RPAREN,
			Text: ")",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "BoolValue",
		},
		{
			Type: T_BOOL,
			Text: "boolean",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "Salary",
		},
		{
			Type: T_BIGINT,
			Text: "bigint",
		},
		{
			Type: RPAREN,
			Text: ")",
		},
		{
			Type: SEMICOLON,
			Text: ";",
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
			Text: "CREATE",
		},
		{
			Type: DATABASE,
			Text: "DATABASE",
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

	const src = `INSERT INTO thedatabase (column1, column2, column3) VALUES (1, 'value2', 'value3')`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: INSERT,
			Text: "INSERT",
		},
		{
			Type: INTO,
			Text: "INTO",
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
		{
			Type: LPAREN,
			Text: "(",
		},
		{
			Type: IDENT,
			Text: "column1",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "column2",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "column3",
		},
		{
			Type: RPAREN,
			Text: ")",
		},
		{
			Type: VALUES,
			Text: "VALUES",
		},
		{
			Type: LPAREN,
			Text: "(",
		},
		{
			Type: INT,
			Text: "1",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: STR,
			Text: "value2",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: STR,
			Text: "value3",
		},
		{
			Type: RPAREN,
			Text: ")",
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

	const src = `UPDATE thedatabase SET column1 = 'val1', column2 = 'val2', column3 = 'val3' WHERE id = 4`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: UPDATE,
			Text: "UPDATE",
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
		{
			Type: SET,
			Text: "SET",
		},
		{
			Type: IDENT,
			Text: "column1",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: STR,
			Text: "val1",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "column2",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: STR,
			Text: "val2",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "column3",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: STR,
			Text: "val3",
		},
		{
			Type: WHERE,
			Text: "WHERE",
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
			Text: "=",
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
			Text: "USE",
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
			Text: "SELECT",
		},
		{
			Type: IDENT,
			Text: "field_1",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: IDENT,
			Text: "field_2",
		},
		{
			Type: FROM,
			Text: "FROM",
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: WHERE,
			Text: "WHERE",
		},
		{
			Type: IDENT,
			Text: "ident",
		},
		{
			Type: EQ,
			Text: "=",
		},
		{
			Type: STR,
			Text: "\"some literal\"",
		},
		{
			Type: OR,
			Text: "OR",
		},
		{
			Type: IDENT,
			Text: "ident2",
		},
		{
			Type: EQ,
			Text: "=",
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

func TestScanDelete(t *testing.T) {

	const src = `DELETE FROM thedatabase WHERE id = 4`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: DELETE,
			Text: "DELETE",
		},
		{
			Type: FROM,
			Text: "FROM",
		},
		{
			Type: IDENT,
			Text: "thedatabase",
		},
		{
			Type: WHERE,
			Text: "WHERE",
		},
		{
			Type: IDENT,
			Text: "id",
		},
		{
			Type: EQ,
			Text: "=",
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

func TestScanComparisonOperators(t *testing.T) {

	cases := []struct {
		input  string
		expect TokenType
	}{
		{
			input:  `>`,
			expect: GT,
		},
		{
			input:  `>=`,
			expect: GTE,
		},
		{
			input:  `<`,
			expect: LT,
		},
		{
			input:  `<=`,
			expect: LTE,
		},
	}

	for _, test := range cases {

		ts := NewTokenScanner(strings.NewReader(test.input))

		if !ts.Next() {
			t.Error("ran out of tokens")
		}
		actual := ts.Cur()
		if test.expect != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[test.expect], Tokens[actual.Type]))
		}
		if ts.Next() {
			t.Errorf("there are still tokens that remain in scanner. next: %s", ts.Cur().Text)
		}
	}
}

func TestScanSelectCount(t *testing.T) {

	cases := []struct {
		input  string
		expect TokenType
	}{
		{
			input:  `COUNT`,
			expect: COUNT,
		},
		{
			input:  `(`,
			expect: LPAREN,
		},
		{
			input:  `*`,
			expect: ASTRSK,
		},
		{
			input:  `)`,
			expect: RPAREN,
		},
	}

	for _, test := range cases {

		ts := NewTokenScanner(strings.NewReader(test.input))

		if !ts.Next() {
			t.Error("ran out of tokens")
		}
		actual := ts.Cur()
		if test.expect != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[test.expect], Tokens[actual.Type]))
		}
		if ts.Next() {
			t.Errorf("there are still tokens that remain in scanner. next: %s", ts.Cur().Text)
		}
	}
}

func TestScanSelectBoolean(t *testing.T) {

	const src = `SELECT true, false`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: SELECT,
			Text: "SELECT",
		},
		{
			Type: TRUE,
			Text: "true",
		},
		{
			Type: COMMA,
			Text: ",",
		},
		{
			Type: FALSE,
			Text: "false",
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

func TestScanMalformedSQL(t *testing.T) {

	// This test replicates a situation where someone entered a DDL query that
	// accidentally contained curly braces instead of parens. This caused the
	// scanner to throw a runtime error because it attempted to strip quotes
	// from a string containing a single curly brace character.

	const src = `
		CREATE TABLE Persons {
			PersonID int
		};
	`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: CREATE,
			Text: "CREATE",
		},
		{
			Type: TABLE,
			Text: "TABLE",
		},
		{
			Type: IDENT,
			Text: "Persons",
		},
		{
			Type: STR,
			Text: "{",
		},
		{
			Type: IDENT,
			Text: "PersonID",
		},
		{
			Type: T_INT,
			Text: "int",
		},
		{
			Type: STR,
			Text: "}",
		},
		{
			Type: SEMICOLON,
			Text: ";",
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

func TestScanAggregation(t *testing.T) {

	cases := []struct {
		input  string
		expect TokenType
	}{
		{
			input:  `GROUP`,
			expect: GROUP,
		},
		{
			input:  `BY`,
			expect: BY,
		},
		{
			input:  `AVG`,
			expect: AVG,
		},
	}

	for _, test := range cases {

		ts := NewTokenScanner(strings.NewReader(test.input))

		if !ts.Next() {
			t.Error("ran out of tokens")
		}
		actual := ts.Cur()
		if test.expect != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[test.expect], Tokens[actual.Type]))
		}
		if ts.Next() {
			t.Errorf("there are still tokens that remain in scanner. next: %s", ts.Cur().Text)
		}
	}
}
