package sql

import (
	"errors"
	"fmt"
	"strings"
)

type JoinType uint8

const (
	FULL_JOIN = iota
	LEFT_JOIN
	RIGHT_JOIN
	INNER_JOIN
)

var (
	ErrNegativeLimit  = errors.New("LIMIT clause can not be negative")
	ErrNegativeOffset = errors.New("OFFSET clause can not be negative")
)

type Select struct {
	SelectList
	TableExpression
	SortSpecificationList []SortSpecification
	LimitOffsetClause
}

type TableExpression struct {
	FromClause
	WhereClause interface{}
}

type SortSpecification struct {
	SortKey               ValueExpression
	OrderingSpecification Token
}

type LimitOffsetClause struct {
	LimitActive  bool
	OffsetActive bool
	Limit        int32
	Offset       int32
}

type ValueExpression struct {
	Qualifier  interface{}
	ColumnName Token
}

type TableName struct {
	CorrelationName interface{}
	Name            string
}

type Pattern string
type SelectList []ValueExpression
type FromClause []TableReference

// TableReference is one of TableName or JoinedTable
type TableReference interface{}

// TableReference is one of CrossJoin (tbd) or QualifiedJoin
type JoinedTable interface{}

type QualifiedJoin struct {
	LHS TableReference
	JoinType
	RHS           TableReference
	JoinCondition interface{}
}

type WhereClause struct {
	SearchCondition interface{}
}

type SearchCondition struct {
	LHS interface{}
	RHS interface{}
}

type BooleanTerm struct {
	LHS Predicate
	RHS interface{}
}

type Predicate struct {
	ComparisonPredicate
}

type ComparisonPredicate struct {
	LHS    interface{}
	CompOp TokenType
	RHS    interface{}
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
	Len  int32
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

type UpdateStatementSearched struct {
	TableName string
	Set       []SetClause
	Where     interface{}
}

type SetClause struct {
	ObjectColumn string
	UpdateSource ValueExpression
}

type UseStatement struct {
	DBName string
}

func (p *Parser) Parse() (interface{}, error) {
	if err := p.requireMatch(CREATE, SELECT, INSERT, USE, UPDATE); err != nil {
		return nil, err
	}
	switch p.Prev().Type {
	case CREATE:
		return p.Create()
	case SELECT:
		return p.Select()
	case INSERT:
		return p.Insert()
	case UPDATE:
		return p.Update()
	case USE:
		return p.Use()
	default:
		panic("unhandled type")
	}
}

func (p *Parser) Create() (interface{}, error) {
	if err := p.requireMatch(DATABASE, TABLE); err != nil {
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
	if err := p.requireMatch(IDENT); err != nil {
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

	if err := p.requireMatch(LPAREN); err != nil {
		return ret, err
	}

	for p.match(IDENT) {

		te := TableElement{
			ColumnDefinition: ColumnDefinition{
				Name: p.Prev().Text,
			},
		}

		if err := p.requireMatch(T_INT, T_VARCHAR); err != nil {
			return ret, err
		}

		switch p.Prev().Type {
		case T_INT:
			te.ColumnDefinition.DataType = NumericType{}
		case T_VARCHAR:
			cst := CharacterStringType{
				Type: p.Prev().Type,
			}
			if err := p.requireMatch(LPAREN); err != nil {
				return ret, err
			}
			if intVal, err := p.requireInt(); err != nil {
				return ret, err
			} else {
				cst.Len = intVal
			}
			if err := p.requireMatch(RPAREN); err != nil {
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

	if err := p.requireMatch(RPAREN); err != nil {
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

	sel.SortSpecificationList, err = p.SortSpecificationList()
	if err != nil {
		return sel, err
	}

	sel.LimitOffsetClause, err = p.LimitOffsetClause()
	if err != nil {
		return sel, err
	}

	return sel, nil
}

func (p *Parser) SortSpecificationList() ([]SortSpecification, error) {
	var ss []SortSpecification

	if !p.match(ORDER) {
		return ss, nil
	}
	if err := p.requireMatch(BY); err != nil {
		return ss, err
	}

	for p.match(IDENT) {
		s := SortSpecification{
			OrderingSpecification: Token{
				Type: ASC,
			},
		}
		var err error
		s.SortKey, err = p.ValueExpression()
		if err != nil {
			return ss, err
		}

		if p.match(ASC, DESC) {
			s.OrderingSpecification = p.Prev()
		}

		ss = append(ss, s)

		if !p.match(COMMA) {
			break
		}
	}

	return ss, nil
}

func (p *Parser) LimitOffsetClause() (LimitOffsetClause, error) {
	lc := LimitOffsetClause{}
	var err error

	for p.match(LIMIT, OFFSET) {
		switch {
		case p.Prev().Type == LIMIT && !lc.LimitActive:
			lc.LimitActive = true
			if lc.Limit, err = p.requireInt(); err != nil {
				return lc, err
			}
		case p.Prev().Type == OFFSET && !lc.OffsetActive:
			lc.OffsetActive = true
			if lc.Offset, err = p.requireInt(); err != nil {
				return lc, err
			}
		}
	}

	switch {
	case lc.Limit < 0:
		return lc, ErrNegativeLimit
	case lc.Offset < 0:
		return lc, ErrNegativeOffset
	}

	return lc, err
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
	fc := FromClause{}

	if err := p.requireMatch(FROM); err != nil {
		return fc, err
	}

	tn, err := p.TableName()
	if err != nil {
		return fc, err
	}

	tblRef := TableReference(tn)

	for p.curType(JOIN, LEFT, RIGHT, INNER) {
		var jt JoinType
		switch {
		case p.match(LEFT):
			jt = LEFT_JOIN
		case p.match(RIGHT):
			jt = RIGHT_JOIN
		case p.match(INNER):
			fallthrough
		default:
			jt = INNER_JOIN
		}

		if err := p.requireMatch(JOIN); err != nil {
			return fc, err
		}

		var rhs TableReference
		rhs, err = p.TableName()
		if err != nil {
			return fc, err
		}

		qj := QualifiedJoin{
			LHS:      tblRef,
			RHS:      rhs,
			JoinType: jt,
		}

		if err := p.requireMatch(ON); err != nil {
			return fc, err
		}

		var err error
		qj.JoinCondition, err = p.OrCondition()
		if err != nil {
			return fc, err
		}

		tblRef = qj
	}

	fc = append(fc, tblRef)

	return fc, nil
}

func (p *Parser) WhereClause() (interface{}, error) {
	if !p.match(WHERE) {
		return nil, nil
	}

	wc := WhereClause{}

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
		ac := SearchCondition{LHS: ret.(Predicate)}
		ac.RHS, err = p.OrCondition()
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
		ac := BooleanTerm{LHS: ret.(Predicate)}
		ac.RHS, err = p.AndCondition()
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

	if err := p.requireMatch(STR, ASTRSK, IDENT, INT); err != nil {
		return cp, err
	}

	cp.LHS, err = p.ValueExpression()
	if err != nil {
		return cp, err
	}

	if err := p.requireMatch(EQ, NEQ, LT, GT, LTE, GTE); err != nil {
		return cp, err
	}

	cp.CompOp = p.Prev().Type

	if err := p.requireMatch(STR, ASTRSK, IDENT, INT); err != nil {
		return cp, err
	}

	cp.RHS, err = p.ValueExpression()
	if err != nil {
		return cp, err
	}

	return cp, nil
}

func (p *Parser) ValueExpression() (ValueExpression, error) {
	ve := ValueExpression{}

	if p.Prev().Type == IDENT && p.curType(DOT) {
		ve.Qualifier = p.Prev()
		if !p.match(DOT) {
			panic("should have matched a DOT")
		}
		if err := p.requireMatch(IDENT); err != nil {
			return ve, err
		}
	}

	ve.ColumnName = p.Prev()

	return ve, nil
}

func (p *Parser) SelectList() (SelectList, error) {
	sl := SelectList{}

	for p.match(STR, ASTRSK, IDENT, INT) {
		ve, err := p.ValueExpression()
		if err != nil {
			return sl, err
		}

		sl = append(sl, ve)

		if !p.match(COMMA, WHERE) {
			break
		}
	}

	return sl, nil
}

func (p *Parser) TableName() (TableName, error) {
	tn := TableName{}

	if err := p.requireMatch(IDENT); err != nil {
		return tn, err
	}

	tn.Name = p.Prev().Text

	if p.match(IDENT) {
		tn.CorrelationName = p.Prev().Text
	}

	return tn, nil
}

func (p *Parser) Insert() (InsertStatement, error) {
	is := InsertStatement{}

	if err := p.requireMatch(INTO); err != nil {
		return is, err
	}

	if err := p.requireMatch(IDENT); err != nil {
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
		if err := p.requireMatch(RPAREN); err != nil {
			return is, err
		}
	}

	if err := p.requireMatch(VALUES); err != nil {
		return is, err
	}

	if err := p.requireMatch(LPAREN); err != nil {
		return is, err
	}
	var cols []interface{}
	for p.match(STR, INT) {
		val, err := p.Prev().Val()
		if err != nil {
			return is, err
		}
		cols = append(cols, val)
		if !p.match(COMMA) {
			break
		}
	}
	is.InsertColumnsAndSource.TableValueConstructor.Columns = cols
	if err := p.requireMatch(RPAREN); err != nil {
		return is, err
	}

	return is, nil
}

func (p *Parser) Update() (UpdateStatementSearched, error) {
	us := UpdateStatementSearched{}

	if err := p.requireMatch(IDENT); err != nil {
		return us, err
	}

	us.TableName = p.Prev().Text

	if err := p.requireMatch(SET); err != nil {
		return us, err
	}

	for p.match(IDENT) {
		sc := SetClause{}
		sc.ObjectColumn = p.Prev().Text

		if err := p.requireMatch(EQ); err != nil {
			return us, err
		}

		if err := p.requireMatch(STR, INT); err != nil {
			return us, err
		}

		var err error
		sc.UpdateSource, err = p.ValueExpression()
		if err != nil {
			return us, err
		}

		us.Set = append(us.Set, sc)

		if !p.match(COMMA) {
			break
		}
	}

	var err error
	us.Where, err = p.WhereClause()
	if err != nil {
		return us, err
	}

	return us, nil
}

func (p *Parser) Use() (UseStatement, error) {
	us := UseStatement{}

	if err := p.requireMatch(IDENT); err != nil {
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

func (p *Parser) requireMatch(types ...TokenType) error {
	if p.match(types...) {
		return nil
	}
	return p.unexpectedTypeErr(types...)
}

func (p *Parser) curType(types ...TokenType) bool {
	return hasType(p.Cur().Type, types...)
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

func (p *Parser) requireInt() (int32, error) {
	if err := p.requireMatch(INT); err != nil {
		return 0, err
	}
	val, err := p.Prev().Val()
	return val.(int32), err
}
