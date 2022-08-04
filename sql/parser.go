package sql

import (
	"fmt"
	"strconv"
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

type CreateTable struct {
	Name     string
	Elements []TableElement
}

type TableElement struct {
	ColumnDefinition
}

type ColumnDefinition struct {
	DataType interface{}
	Name     string
}

type CharacterStringType struct {
	Len  int
	Type TokenType
}

type NumericType struct {
}

type Parser struct {
	TokenList
}

type CreateDatabase struct {
	Name string
}

type InsertStatement struct {
	TableName string
	InsertColumnsAndSource
}

type InsertColumnsAndSource struct {
	InsertColumnList
	TableValueConstructor
}

type InsertColumnList struct {
	ColumnNames []string
}

type TableValueConstructor struct {
	Columns []interface{}
}

type UseStatement struct {
	DBName string
}

func (p *Parser) Parse() (interface{}, error) {
	if ok, err := p.requireMatch(CREATE, SELECT, INSERT, USE); !ok {
		return nil, err
	}
	switch p.Prev().Type {
	case CREATE:
		return p.Create()
	case SELECT:
		return p.Select()
	case INSERT:
		return p.Insert()
	case USE:
		return p.Use()
	default:
		panic("unhandled type")
	}
}

func (p *Parser) Create() (interface{}, error) {
	if ok, err := p.requireMatch(DATABASE, TABLE); !ok {
		return nil, err
	}
	switch p.Prev().Type {
	case DATABASE:
		return p.CreateDatabase()
	case TABLE:
		return p.CreateTable()
	default:
		panic("unhandled type")
	}
}

func (p *Parser) CreateDatabase() (CreateDatabase, error) {
	cd := CreateDatabase{}
	if ok, err := p.requireMatch(IDENT); !ok {
		return cd, err
	}
	cd.Name = p.Prev().Text
	return cd, nil
}

func (p *Parser) CreateTable() (CreateTable, error) {
	ct := CreateTable{}
	var err error

	if p.match(IDENT) {
		ct.Name = p.Prev().Text
	}

	ct.Elements, err = p.TableElements()
	if err != nil {
		return ct, err
	}

	return ct, nil
}

func (p *Parser) TableElements() ([]TableElement, error) {

	var ret []TableElement

	if ok, err := p.requireMatch(LPAREN); !ok {
		return ret, err
	}

	for p.match(IDENT) {

		te := TableElement{
			ColumnDefinition: ColumnDefinition{
				Name: p.Prev().Text,
			},
		}

		if ok, err := p.requireMatch(T_INT, T_VARCHAR); !ok {
			return ret, err
		}

		switch p.Prev().Type {
		case T_INT:
			te.ColumnDefinition.DataType = NumericType{}
		case T_VARCHAR:
			cst := CharacterStringType{
				Type: p.Prev().Type,
			}
			if ok, err := p.requireMatch(LPAREN); !ok {
				return ret, err
			}
			if ok, err := p.requireMatch(INT); !ok {
				return ret, err
			}
			intVal, err := strconv.Atoi(p.Prev().Text)
			if err != nil {
				return ret, err
			}
			cst.Len = intVal
			if ok, err := p.requireMatch(RPAREN); !ok {
				return ret, err
			}
			te.ColumnDefinition.DataType = cst
		default:
			panic("unhandled type")
		}

		ret = append(ret, te)

		if !p.match(COMMA) {
			break
		}
	}

	if ok, err := p.requireMatch(RPAREN); !ok {
		return ret, err
	}

	return ret, nil
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

	if ok, err := p.requireMatch(EQ, NEQ, LT, GT, LTE, GTE); !ok {
		return cp, err
	}

	cp.CompOp = p.Prev().Type

	cp.rhs, err = p.ValueExpression()
	if err != nil {
		return cp, err
	}

	return cp, nil
}

func (p *Parser) ValueExpression() (ValueExpression, error) {
	ret := ValueExpression{}
	if ok, err := p.requireMatch(STR, IDENT, INT); !ok {
		return ret, err
	}
	ret.Token = p.Prev()
	return ret, nil
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

func (p *Parser) Insert() (InsertStatement, error) {
	is := InsertStatement{}

	if ok, err := p.requireMatch(INTO); !ok {
		return is, err
	}

	if ok, err := p.requireMatch(IDENT); !ok {
		return is, err
	}

	is.TableName = p.Prev().Text

	if p.match(LPAREN) {
		var colNames []string
		for p.match(IDENT) {
			colNames = append(colNames, p.Prev().Text)
			if !p.match(COMMA) {
				break
			}
		}
		is.InsertColumnsAndSource.InsertColumnList.ColumnNames = colNames
		if ok, err := p.requireMatch(RPAREN); !ok {
			return is, err
		}
	}

	if ok, err := p.requireMatch(VALUES); !ok {
		return is, err
	}

	if ok, err := p.requireMatch(LPAREN); !ok {
		return is, err
	}
	var cols []interface{}
	for p.match(STR, INT) {
		var val interface{}
		switch p.Prev().Type {
		case STR:
			val = p.Prev().Text
		case INT:
			intVal, err := strconv.Atoi(p.Prev().Text)
			if err != nil {
				return is, err
			}
			val = int32(intVal)
		}
		cols = append(cols, val)
		if !p.match(COMMA) {
			break
		}
	}
	is.InsertColumnsAndSource.TableValueConstructor.Columns = cols
	if ok, err := p.requireMatch(RPAREN); !ok {
		return is, err
	}

	return is, nil
}

func (p *Parser) Use() (UseStatement, error) {
	us := UseStatement{}

	if ok, err := p.requireMatch(IDENT); !ok {
		return us, err
	}

	us.DBName = p.Prev().Text

	return us, nil
}

func (p *Parser) match(types ...TokenType) bool {
	if hasType(p.Cur().Type, types...) {
		p.Advance()
		return true
	}
	return false
}

func (p *Parser) requireMatch(types ...TokenType) (bool, error) {
	if p.match(types...) {
		return true, nil
	}
	return false, p.unexpectedTypeErr(types...)
}

func hasType(targetType TokenType, types ...TokenType) bool {
	for _, tipe := range types {
		if targetType == tipe {
			return true
		}
	}
	return false
}

func (p *Parser) unexpectedTypeErr(types ...TokenType) error {
	unex := Tokens[p.Cur().Type]
	if p.Cur().Text != "" {
		unex = fmt.Sprintf("%s (%s)", unex, p.Cur().Text)
	}
	var typeNames []string
	for _, tipe := range types {
		typeNames = append(typeNames, Tokens[tipe])
	}
	return fmt.Errorf("unexpected token type: %s. expected: %s", unex, strings.Join(typeNames, ", "))
}
