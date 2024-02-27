package sql

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	IDENT = iota
	literal_start
	INT
	STR
	reserved_word_start
	TRUE
	FALSE
	literal_end

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

	AS
	ASC
	AVG
	BEGIN
	BY
	CASE
	COMMA
	COMMIT
	COUNT
	CREATE
	DATABASE
	DELETE
	DESC
	DISTINCT
	DOT
	ELSE
	END
	EXISTS
	FROM
	FULL
	GROUP
	HAVING
	IN
	INNER
	INSERT
	INTO
	JOIN
	LEFT
	LIKE
	LIMIT
	MAX
	MIN
	NOT
	NULL
	OFFSET
	ON
	ORDER
	OUTER
	RIGHT
	SELECT
	SEMICOLON
	SET
	SHOW
	SUM
	T_BOOL
	T_INT
	T_BIGINT
	T_VARCHAR
	TABLE
	THEN
	UNION
	UNIQUE
	UPDATE
	USE
	VALUES
	WHEN
	WHERE
	WITH
	reserved_word_end
)

type TokenType int

func (t TokenType) IsReservedWord() bool {
	return reserved_word_start < t && t < reserved_word_end
}

func (t TokenType) IsLiteral() bool {
	return literal_start < t && t < literal_end
}

var Tokens = map[TokenType]string{
	INT:   "an integer",
	STR:   "a string",
	TRUE:  "TRUE",
	FALSE: "FALSE",

	IDENT: "an identifier",

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
	ASC:       "ASC",
	AVG:       "AVG",
	BEGIN:     "BEGIN",
	BY:        "BY",
	CASE:      "CASE",
	COMMA:     ",",
	COMMIT:    "COMMIT",
	COUNT:     "COUNT",
	CREATE:    "CREATE",
	DATABASE:  "DATABASE",
	DELETE:    "DELETE",
	DESC:      "DESC",
	DISTINCT:  "DISTINCT",
	DOT:       ".",
	ELSE:      "ELSE",
	END:       "END",
	EXISTS:    "EXISTS",
	FROM:      "FROM",
	FULL:      "FULL",
	GROUP:     "GROUP",
	HAVING:    "HAVING",
	IN:        "IN",
	INNER:     "INNER",
	INSERT:    "INSERT",
	INTO:      "INTO",
	JOIN:      "JOIN",
	LEFT:      "LEFT",
	LIKE:      "LIKE",
	LIMIT:     "LIMIT",
	MAX:       "MAX",
	MIN:       "MIN",
	NOT:       "NOT",
	NULL:      "NULL",
	OFFSET:    "OFFSET",
	ON:        "ON",
	ORDER:     "ORDER",
	OUTER:     "OUTER",
	RIGHT:     "RIGHT",
	SELECT:    "SELECT",
	SEMICOLON: ";",
	SET:       "SET",
	SHOW:      "SHOW",
	SUM:       "SUM",
	T_BOOL:    "BOOLEAN",
	T_INT:     "INT",
	T_BIGINT:  "BIGINT",
	T_VARCHAR: "VARCHAR",
	TABLE:     "TABLE",
	THEN:      "THEN",
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

var literals []TokenType

func init() {
	keywords = make(map[string]TokenType)
	for i := TokenType(reserved_word_start) + 1; i < reserved_word_end; i++ {
		// make sure we don't include boundary enums
		if _, ok := Tokens[i]; ok {
			keywords[Tokens[i]] = i
		}
	}
	for i := TokenType(literal_start) + 1; i < literal_end; i++ {
		literals = append(literals, i)
	}
}

type Token struct {
	Type   TokenType
	Line   int
	Column int
	Text   string
}

func (t Token) Val() (interface{}, error) {
	switch t.Type {
	case STR:
		return t.Text, nil
	case INT:
		intVal, err := strconv.Atoi(t.Text)
		if err != nil {
			return nil, err
		}
		return int64(intVal), nil
	case TRUE:
		return true, nil
	case FALSE:
		return false, nil
	}
	return nil, fmt.Errorf("unsupported token type: %v", t)
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
	s   Scanner
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
	case EOF:
		tok.Type = EOF
	case Ident:
		tok.Type = IDENT
		tok.Text = ts.s.TokenText()
		if kw, isKw := keywords[strings.ToUpper(ts.s.TokenText())]; isKw {
			tok.Type = kw
		}
	case Int:
		tok.Type = INT
		tok.Text = ts.s.TokenText()
	case DelimIdent:
		tok.Type = IDENT
		// strip quotes
		tok.Text = ts.s.TokenText()
		tok.Text = tok.Text[1 : len(tok.Text)-1]
	default:
		tok.Text = ts.s.TokenText()
		if kw, isKw := keywords[strings.ToUpper(ts.s.TokenText())]; isKw {
			switch {
			case kw == BANG && ts.s.Peek() == '=':
				tok.Type = NEQ
				ts.Next()
			case kw == GT && ts.s.Peek() == '=':
				tok.Type = GTE
				ts.Next()
			case kw == LT && ts.s.Peek() == '=':
				tok.Type = LTE
				ts.Next()
			default:
				tok.Type = kw
			}
		} else {
			tok.Type = STR
			if ts.cur == String {
				// strip quotes
				tok.Text = tok.Text[1 : len(tok.Text)-1]
			}
		}
	}
	return tok
}

func (ts *tokenScanner) Next() bool {
	ts.cur = ts.s.Scan()
	return ts.cur != EOF
}
