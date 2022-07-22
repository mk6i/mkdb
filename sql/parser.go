package sql

import (
	"fmt"
	"strings"
)

type Select struct {
	SelectList SelectList
	FromList   FromList
	Condition  Condition
}
type Primary struct {
	Token Token
}

type Relation string
type Pattern string
type SelectList []Primary
type FromList []Relation
type Condition struct {
	Left     interface{}
	Right    interface{}
	Operator TokenType
}

type Parser struct {
	*TokenList
}

func (p *Parser) Parse() (interface{}, error) {
	switch p.Cur().Type {
	case SELECT:
		p.Advance()
		return p.ParseSelect()
	default:
		return nil, fmt.Errorf("unexpected token type: %s", Tokens[p.Cur().Type])
	}
}

func (p *Parser) ParseSelect() (Select, error) {
	sel := Select{}

	var err error
	sel.SelectList, err = p.ParseSelectList()
	if err != nil {
		return sel, err
	}

	if err := p.assertType(FROM); err != nil {
		return sel, err
	}

	p.Advance()

	sel.FromList, err = p.ParseFromList()
	if err != nil {
		return sel, err
	}

	if err := p.assertType(WHERE); err != nil {
		return sel, err
	}

	p.Advance()

	sel.Condition, err = p.ParseCondition()
	if err != nil {
		return sel, err
	}

	return sel, nil
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

func (p *Parser) ParseSelectList() (SelectList, error) {
	sl := SelectList{}

	if err := p.assertType(ASTRSK, IDENT, STR); err != nil {
		return sl, err
	}

	for p.hasType(ASTRSK, IDENT, STR) {
		sl = append(sl, Primary{p.Cur()})
		p.Advance()
		if p.hasType(COMMA, WHERE) {
			p.Advance()
		} else {
			break
		}
	}

	return sl, nil
}

func (p *Parser) ParseFromList() (FromList, error) {
	fl := FromList{}

	if err := p.assertType(IDENT); err != nil {
		return fl, err
	}

	for p.hasType(IDENT) {
		fl = append(fl, Relation(p.Cur().Text))
		p.Advance()
	}

	return fl, nil
}

func (p *Parser) ParseCondition() (Condition, error) {
	cnd := Condition{}

	var err error

	cnd.Left, err = p.ParsePrimary()
	if err != nil {
		return cnd, err
	}

	cnd.Operator = p.Cur().Type
	p.Advance()

	switch p.Prev().Type {
	case IN:
		cnd.Right, err = p.ParseSelect()
	case EQ:
		fallthrough
	case LIKE:
		cnd.Right, err = p.ParsePrimary()
	}

	return cnd, err
}

func (p *Parser) ParsePrimary() (Primary, error) {
	ret := Primary{}

	switch p.Cur().Type {
	case STR:
		fallthrough
	case IDENT:
		fallthrough
	case ASTRSK:
		ret.Token = p.Cur()
		p.Advance()
		return ret, nil
	}

	return ret, p.assertType(IDENT, ASTRSK)
}
