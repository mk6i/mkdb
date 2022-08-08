package sql

import (
	"io"
	"strings"
	"text/scanner"
)

const (
	EOF = iota

	literal_start
	IDENT
	INT
	STR
	literal_end

	operator_start
	BANG
	AND
	OR
	ASTRSK
	EQ
	NEQ
	GT
	LT
	LTE
	GTE
	LPAREN
	RPAREN
	operator_end

	keyword_start
	AS
	BEGIN
	BY
	CASE
	COMMA
	COMMIT
	COUNT
	CREATE
	DATABASE
	DISTINCT
	ELSE
	END
	EXISTS
	FALSE
	FROM
	FULL
	GROUP
	HAVING
	IN
	INSERT
	INTO
	JOIN
	LEFT
	LIKE
	MAX
	MIN
	NOT
	NULL
	ON
	ORDER
	OUTER
	SELECT
	SEMICOLON
	SET
	SUM
	T_INT
	T_VARCHAR
	TABLE
	THEN
	TRUE
	UNION
	UNIQUE
	UPDATE
	USE
	VALUES
	WHEN
	WHERE
	WITH
	keyword_end
)

type TokenType int

func (t TokenType) IsKeyword() bool {
	return keyword_start < t && t < keyword_end
}

func (t TokenType) IsLiteral() bool {
	return literal_start < t && t < literal_end
}

func (t TokenType) IsOperator() bool {
	return operator_start < t && t < operator_end
}

var Tokens = map[TokenType]string{
	EOF: "EOF",

	IDENT: "IDENT",
	INT:   "INT",
	STR:   "STR",

	BANG:   "!",
	AND:    "AND",
	OR:     "OR",
	ASTRSK: "*",
	EQ:     "=",
	NEQ:    "!=",
	GT:     ">",
	LT:     "<",
	LTE:    "<=",
	GTE:    ">=",
	LPAREN: "(",
	RPAREN: ")",

	AS:        "AS",
	BEGIN:     "BEGIN",
	BY:        "BY",
	CASE:      "CASE",
	COMMA:     ",",
	COMMIT:    "COMMIT",
	COUNT:     "COUNT",
	CREATE:    "CREATE",
	DATABASE:  "DATABASE",
	DISTINCT:  "DISTINCT",
	ELSE:      "ELSE",
	END:       "END",
	EXISTS:    "EXISTS",
	FALSE:     "FALSE",
	FROM:      "FROM",
	FULL:      "FULL",
	GROUP:     "GROUP",
	HAVING:    "HAVING",
	IN:        "IN",
	INSERT:    "INSERT",
	INTO:      "INTO",
	JOIN:      "JOIN",
	LEFT:      "LEFT",
	LIKE:      "LIKE",
	MAX:       "MAX",
	MIN:       "MIN",
	NOT:       "NOT",
	NULL:      "NULL",
	ON:        "ON",
	ORDER:     "ORDER",
	OUTER:     "OUTER",
	SELECT:    "SELECT",
	SEMICOLON: ";",
	SET:       "SET",
	SUM:       "SUM",
	T_INT:     "INT",
	T_VARCHAR: "VARCHAR",
	TABLE:     "TABLE",
	THEN:      "THEN",
	TRUE:      "TRUE",
	UNION:     "UNION",
	UNIQUE:    "UNIQUE",
	UPDATE:    "UPDATE",
	USE:       "USE",
	VALUES:    "VALUES",
	WHEN:      "WHEN",
	WHERE:     "WHERE",
	WITH:      "WITH",
}

var keywords map[string]TokenType

func init() {
	keywords = make(map[string]TokenType)
	for i := TokenType(operator_start) + 1; i < operator_end; i++ {
		keywords[Tokens[i]] = i
	}
	for i := TokenType(keyword_start) + 1; i < keyword_end; i++ {
		keywords[Tokens[i]] = i
	}
}

type Token struct {
	Type   TokenType
	Line   int
	Column int
	Text   string
}

type TokenList struct {
	tokens []Token
	cur    int
}

var EOFToken = Token{Type: EOF}

func (tl *TokenList) Add(t Token) {
	tl.tokens = append(tl.tokens, t)
}

func (tl *TokenList) Prev() Token {
	if tl.cur == 0 {
		return EOFToken
	}
	return tl.tokens[tl.cur-1]
}

func (tl *TokenList) Cur() Token {
	if tl.cur == len(tl.tokens) {
		return EOFToken
	}
	return tl.tokens[tl.cur]
}

func (tl *TokenList) HasNext() bool {
	return tl.cur < len(tl.tokens)-1
}

func (tl *TokenList) Peek() Token {
	if tl.cur == len(tl.tokens)-1 {
		return EOFToken
	}
	return tl.tokens[tl.cur+1]
}

func (tl *TokenList) Advance() bool {
	if tl.cur == len(tl.tokens) {
		return false
	}
	tl.cur++
	return true
}

type tokenScanner struct {
	s   scanner.Scanner
	cur rune
}

func NewTokenScanner(src io.Reader) *tokenScanner {
	ts := &tokenScanner{}
	ts.s.Init(src)
	return ts
}

func (ts *tokenScanner) Cur() Token {
	tok := Token{
		Column: ts.s.Column,
		Line:   ts.s.Line,
	}
	switch ts.cur {
	case scanner.EOF:
		tok.Type = EOF
	case scanner.Ident:
		if kw, isKw := keywords[strings.ToUpper(ts.s.TokenText())]; isKw {
			tok.Type = kw
		} else {
			tok.Type = IDENT
			tok.Text = ts.s.TokenText()
		}
	case scanner.Int:
		tok.Type = INT
		tok.Text = ts.s.TokenText()
	default:
		if kw, isKw := keywords[strings.ToUpper(ts.s.TokenText())]; isKw {
			if kw == BANG && ts.s.Peek() == '=' {
				tok.Type = NEQ
				ts.Next()
			} else {
				tok.Type = kw
			}
		} else {
			tok.Type = STR
			tok.Text = ts.s.TokenText()
			// strip quotes
			tok.Text = tok.Text[1 : len(tok.Text)-1]
		}
	}
	return tok
}

func (ts *tokenScanner) Next() bool {
	ts.cur = ts.s.Scan()
	return ts.cur != scanner.EOF
}
