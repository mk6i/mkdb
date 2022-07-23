package sql

import (
	"fmt"
	"strings"
)

type Select struct {
	SelectList
	TableExpression
}

type TableExpression struct {
	FromClause
	*WhereClause
}
type Primary struct {
	Token Token
}

type Relation string
type Pattern string
type SelectList []Primary
type FromClause []Relation
type WhereClause struct {
	Left     interface{}
	Right    interface{}
	Operator TokenType
}

type Parser struct {
	*TokenList
}

func (p *Parser) Parse() (interface{}, error) {
	switch {
	case p.match(SELECT):
		return p.Select()
	}
	return nil, fmt.Errorf("unexpected token type: %s", Tokens[p.Cur().Type])
}

func (p *Parser) Select() (Select, error) {
	sel := Select{}
	var err error

	sel.SelectList, err = p.SelectList()
	if err != nil {
		return sel, err
	}

	sel.TableExpression, err = p.TableExpression()
	if err != nil {
		return sel, err
	}

	return sel, nil
}

func (p *Parser) TableExpression() (TableExpression, error) {
	te := TableExpression{}
	var err error

	te.FromClause, err = p.FromClause()
	if err != nil {
		return te, err
	}

	te.WhereClause, err = p.WhereClause()
	if err != nil {
		return te, err
	}

	return te, err
}

func (p *Parser) FromClause() (FromClause, error) {
	fl := FromClause{}

	if !p.match(FROM) {
		return fl, nil
	}

	for p.match(IDENT) {
		fl = append(fl, Relation(p.Prev().Text))
	}

	return fl, nil
}

func (p *Parser) WhereClause() (*WhereClause, error) {
	if !p.match(WHERE) {
		return nil, nil
	}

	wc := &WhereClause{}
	var err error

	wc.Left, err = p.Primary()
	if err != nil {
		return nil, err
	}

	wc.Operator = p.Cur().Type

	switch {
	case p.match(IN):
		wc.Right, err = p.Select()
	case p.match(EQ, LIKE):
		wc.Right, err = p.Primary()
	default:
		err = p.assertType(IN, EQ, LIKE)
	}

	return wc, err
}

func (p *Parser) SelectList() (SelectList, error) {
	sl := SelectList{}

	for p.match(ASTRSK, IDENT, STR) {
		sl = append(sl, Primary{p.Prev()})
		if !p.match(COMMA, WHERE) {
			break
		}
	}

	return sl, nil
}

func (p *Parser) Primary() (Primary, error) {
	ret := Primary{}

	if p.match(STR, IDENT, ASTRSK) {
		ret.Token = p.Prev()
		return ret, nil
	}

	return ret, p.assertType(IDENT, ASTRSK)
}

func (p *Parser) match(types ...TokenType) bool {
	if p.hasType(types...) {
		p.Advance()
		return true
	}
	return false
}

func (p *Parser) hasType(types ...TokenType) bool {
	for _, tipe := range types {
		if p.Cur().Type == tipe {
			return true
		}
	}
	return false
}

func (p *Parser) assertType(types ...TokenType) error {
	if !p.hasType(types...) {
		unex := Tokens[p.Cur().Type]
		if p.Cur().Text != "" {
			unex = fmt.Sprintf("%s (%s)", unex, p.Cur().Text)
		}
		var typeNames []string
		for _, tipe := range types {
			typeNames = append(typeNames, Tokens[tipe])
		}
		return fmt.Errorf("unexpected token type: %s. expected: %s", unex, strings.Join(typeNames, ","))
	}
	return nil
}
