package sql

import (
	"fmt"
	"strings"
	"testing"
)

func TestScan(t *testing.T) {

	const src = `SELECT * FROM the_table WHERE ident = "some literal" OR ident2 = 12`

	ts := NewTokenScanner(strings.NewReader(src))

	expected := []Token{
		{
			Type: IDENT,
			Text: "SELECT",
		},
		{
			Type: ASTRSK,
			Text: "*",
		},
		{
			Type: IDENT,
			Text: "FROM",
		},
		{
			Type: IDENT,
			Text: "the_table",
		},
		{
			Type: IDENT,
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
			Type: IDENT,
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

	for _, exp := range expected {
		if !ts.Next() {
			t.Error("ran out of tokens")
		}
		actual := ts.Cur()
		if exp.Text != actual.Text {
			t.Errorf(fmt.Sprintf("token text does not match. expected: %s actual: %s", exp.Text, actual.Text))
		}
		if exp.Type != actual.Type {
			t.Errorf(fmt.Sprintf("token type does not match. expected: %s actual: %s", Tokens[exp.Type], Tokens[actual.Type]))
		}
	}
	if ts.Next() {
		t.Error("there are still tokens that remain in scanner")
	}
}
