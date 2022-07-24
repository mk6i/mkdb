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
	WhereClause interface{}
}
type ValueExpression struct {
	Token Token
}

type Relation string
type Pattern string
type SelectList []ValueExpression
type FromClause []Relation
type WhereClause struct {
	SearchCondition interface{}
}

type SearchCondition struct {
	lhs interface{}
	rhs interface{}
}

type BooleanTerm struct {
	lhs Predicate
	rhs interface{}
}

type Predicate struct {
	ComparisonPredicate
}

type ComparisonPredicate struct {
	lhs    interface{}
	CompOp TokenType
	rhs    interface{}
}

type Parser struct {
	TokenList
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

func (p *Parser) WhereClause() (WhereClause, error) {
	wc := WhereClause{}

	if !p.match(WHERE) {
		return wc, nil
	}

	var err error
	wc.SearchCondition, err = p.OrCondition()

	return wc, err
}

func (p *Parser) OrCondition() (interface{}, error) {
	var ret interface{}

	ret, err := p.AndCondition()
	if err != nil {
		return nil, err
	}

	for p.match(OR) {
		ac := SearchCondition{lhs: ret.(Predicate)}
		ac.rhs, err = p.OrCondition()
		if err != nil {
			return nil, err
		}
		ret = ac
	}

	return ret, nil
}

func (p *Parser) AndCondition() (interface{}, error) {
	var ret interface{}

	ret, err := p.Predicate()
	if err != nil {
		return nil, err
	}

	for p.match(AND) {
		ac := BooleanTerm{lhs: ret.(Predicate)}
		ac.rhs, err = p.AndCondition()
		if err != nil {
			return nil, err
		}
		ret = ac
	}

	return ret, nil
}

func (p *Parser) Predicate() (interface{}, error) {
	pred, err := p.ComparisonPredicate()
	if err != nil {
		return nil, err
	}
	return Predicate{
		pred,
	}, nil
}

func (p *Parser) ComparisonPredicate() (ComparisonPredicate, error) {
	cp := ComparisonPredicate{}
	var err error

	cp.lhs, err = p.ValueExpression()
	if err != nil {
		return cp, err
	}

	if p.match(EQ, NEQ, LT, GT, LTE, GTE) {
		cp.CompOp = p.Prev().Type
	} else {
		return cp, p.assertType(EQ, NEQ, LT, GT, LTE, GTE)
	}

	cp.rhs, err = p.ValueExpression()
	if err != nil {
		return cp, err
	}

	return cp, nil

}

func (p *Parser) ValueExpression() (ValueExpression, error) {
	ret := ValueExpression{}

	if p.match(STR, IDENT, INT) {
		ret.Token = p.Prev()
		return ret, nil
	}

	return ret, p.assertType(IDENT, ASTRSK)
}

func (p *Parser) SelectList() (SelectList, error) {
	sl := SelectList{}

	for p.match(ASTRSK, IDENT, STR) {
		sl = append(sl, ValueExpression{p.Prev()})
		if !p.match(COMMA, WHERE) {
			break
		}
	}

	return sl, nil
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
