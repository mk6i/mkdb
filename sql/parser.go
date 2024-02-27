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
	ErrAmbiguousGroupByColumn = errors.New("group by column is ambiguous")
	ErrInvalidGroupByColumn   = errors.New("cannot include column in result set without grouping or aggregation")
	ErrNegativeLimit          = errors.New("LIMIT clause can not be negative")
	ErrNegativeOffset         = errors.New("OFFSET clause can not be negative")
	ErrSyntax                 = errors.New("syntax error")
	ErrTmpUnsupportedSyntax   = errors.New("temporarily unsupported syntax")
	ErrUnexpectedToken        = errors.New("unexpected token")
)

func syntaxErr(t Token) error {
	return fmt.Errorf("%w around `%s`", ErrSyntax, t.Text)
}

func invalidGroupByColumnErr(cr DerivedColumn) error {
	return fmt.Errorf("%w: `%s`", ErrInvalidGroupByColumn, cr)
}

type Select struct {
	SelectList
	TableExpression
	SortSpecificationList []SortSpecification
	LimitOffsetClause
}

type TableExpression struct {
	FromClause
	WhereClause   interface{}
	GroupByClause []ColumnReference
}

type SortSpecification struct {
	SortKey               ColumnReference
	OrderingSpecification Token
}

type LimitOffsetClause struct {
	LimitActive  bool
	OffsetActive bool
	Limit        int
	Offset       int
}

type ColumnReference struct {
	Qualifier  string
	ColumnName string
}

// Equals returns true if column reference v and column reference rhs have the
// same qualifiers and column names.
func (v ColumnReference) Equals(rhs ColumnReference) bool {
	if (v.Qualifier == "") != (rhs.Qualifier == "") {
		return false
	}
	// at this point, either both qualifiers are empty or not
	//if v.Qualifier != "" && v.Qualifier != rhs.Qualifier {
	if v.Qualifier != rhs.Qualifier {
		return false
	}
	if v.ColumnName != rhs.ColumnName {
		return false
	}
	return true
}

func (v ColumnReference) String() string {
	if v.Qualifier != "" {
		return fmt.Sprintf("%v.%v", v.Qualifier, v.ColumnName)
	}
	return v.ColumnName
}

type TableName struct {
	CorrelationName interface{}
	Name            string
}

type DerivedColumn struct {
	ValueExpressionPrimary
	AsClause string
}

func (d DerivedColumn) IsColumnReference() bool {
	_, ok := d.ValueExpressionPrimary.(ColumnReference)
	return ok
}

// Matches returns true if column reference rhs matches the column reference in
// derived column d, based on column names, qualifiers
// (i.e. SELECT qualifier.field) and aliases (i.e. SELECT field as alias).
func (d DerivedColumn) Matches(rhs ColumnReference) bool {
	lhs, isCr := d.ValueExpressionPrimary.(ColumnReference)
	if !isCr {
		return false
	}

	switch {
	case lhs.Equals(rhs):
		// col name and qualifiers are identical
		fallthrough
	case d.AsClause == rhs.ColumnName:
		// derived column alias matches column name
		fallthrough
	case lhs.ColumnName == rhs.ColumnName && rhs.Qualifier == "":
		// column names are the same. because rhs qualifier is unspecified,
		// qualifier comparison is ignored
		return true
	}

	return false
}

// ValueExpressionPrimary is one of ColumnReference or Count or Avg
type ValueExpressionPrimary any

type Pattern string
type SelectList []DerivedColumn

func (s SelectList) HasAggrFunc() bool {
	for _, selectCol := range s {
		if _, isAggr := selectCol.ValueExpressionPrimary.(Count); isAggr {
			return true
		}
		if _, isAggr := selectCol.ValueExpressionPrimary.(Average); isAggr {
			return true
		}
	}
	return false
}

type FromClause []TableReference

// SimpleTable is one of QuerySpecification, TableValueConstructor,
// or ExplicitTable (tbd)
type SimpleTable interface{}

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

type BooleanType struct {
}

type CharacterStringType struct {
	Len  int64
	Type TokenType
}

type NumericType struct {
}

type BigIntType struct {
}

type Parser struct {
	TokenList
}

type ShowDatabase struct{}

type CreateDatabase struct {
	Name string
}

type InsertStatement struct {
	TableName string
	InsertColumnsAndSource
}

type InsertColumnsAndSource struct {
	InsertColumnList
	QueryExpression SimpleTable
}

type InsertColumnList struct {
	ColumnNames []string
}

type TableValueConstructor struct {
	TableValueConstructorList []RowValueConstructor
}

type RowValueConstructor struct {
	RowValueConstructorList []interface{}
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

type DeleteStatementSearched struct {
	TableName   string
	WhereClause interface{}
}

type Count struct {
	ValueExpression
}

type Average struct {
	ValueExpression
}

type Asterisk struct{}

func (p *Parser) Parse() (interface{}, error) {
	cur := p.Cur()
	p.Advance()
	switch cur.Type {
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
	case DELETE:
		return p.Delete()
	case SHOW:
		return p.Show()
	default:
		return nil, syntaxErr(cur)
	}
}

func (p *Parser) Show() (interface{}, error) {
	cur := p.Cur()
	p.Advance()
	switch cur.Type {
	case DATABASE:
		return p.ShowDatabase()
	case IDENT:
		if strings.ToLower(cur.Text) == "databases" {
			return p.ShowDatabase()
		}
	}
	return nil, syntaxErr(cur)
}

func (p *Parser) ShowDatabase() (ShowDatabase, error) {
	return ShowDatabase{}, nil
}

func (p *Parser) Create() (interface{}, error) {
	cur := p.Cur()
	p.Advance()
	switch cur.Type {
	case DATABASE:
		return p.CreateDatabase()
	case TABLE:
		return p.CreateTable()
	default:
		return nil, syntaxErr(cur)
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

		cur := p.Cur()
		p.Advance()

		switch cur.Type {
		case T_INT:
			te.ColumnDefinition.DataType = NumericType{}
		case T_BIGINT:
			te.ColumnDefinition.DataType = BigIntType{}
		case T_VARCHAR:
			cst := CharacterStringType{
				Type: cur.Type,
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
		case T_BOOL:
			te.ColumnDefinition.DataType = BooleanType{}
		default:
			return ret, syntaxErr(cur)
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

	// todo: can hasFromClause be replaced with a nil-check?
	var hasFromClause bool
	sel.TableExpression, hasFromClause, err = p.TableExpression()
	if err != nil {
		return sel, err
	}

	// check for missing FROM clause. FROM is not required if query ends with
	// select list.
	if !hasFromClause && p.HasNext() {
		return sel, p.requireMatch(FROM)
	}

	if err := validateGroupByFields(sel); err != nil {
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

// validateGroupByFields ensures that SELECT columns correctly match GROUP BY
// columns
func validateGroupByFields(s Select) error {
	if !s.HasAggrFunc() && len(s.GroupByClause) == 0 {
		return nil
	}

	// check that each SELECT column has a corresponding GROUP BY column
	//
	// invalid:
	// SELECT count(*), field_1, field_2
	// FROM some_table
	// GROUP BY field_1
	for _, derivedCol := range s.SelectList {
		if !derivedCol.IsColumnReference() {
			continue
		}
		var hasMatch bool
		for _, groupByCol := range s.GroupByClause {
			if derivedCol.Matches(groupByCol) {
				hasMatch = true
				break
			}
		}
		if !hasMatch {
			return invalidGroupByColumnErr(derivedCol)
		}
	}

	// check that each GROUP BY column corresponds to at most one SELECT column
	//
	// invalid:
	// SELECT count(*), s1.year, s2.year
	// FROM s1
	// JOIN s2 ON s1.number = s2.number
	// GROUP BY year;
	for _, groupByCol := range s.GroupByClause {
		var hasMatch bool
		for _, derivedCol := range s.SelectList {
			if !derivedCol.IsColumnReference() {
				continue
			}
			if derivedCol.Matches(groupByCol) {
				if hasMatch {
					return ErrAmbiguousGroupByColumn
				}
				hasMatch = true
			}
		}
	}

	return nil
}

func (p *Parser) SortSpecificationList() ([]SortSpecification, error) {
	var ss []SortSpecification

	if !p.match(ORDER) {
		return ss, nil
	}
	if err := p.requireMatch(BY); err != nil {
		return ss, err
	}

	for {
		ok, cr, err := p.ColumnReference()
		if err != nil {
			return ss, err
		}
		if !ok {
			return ss, p.unexpectedTypeErr(IDENT)
		}

		s := SortSpecification{
			OrderingSpecification: Token{
				Type: ASC,
			},
			SortKey: cr,
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
			limit, err := p.requireInt()
			if err != nil {
				return lc, err
			}
			lc.Limit = int(limit)
		case p.Prev().Type == OFFSET && !lc.OffsetActive:
			lc.OffsetActive = true
			offset, err := p.requireInt()
			if err != nil {
				return lc, err
			}
			lc.Offset = int(offset)
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

func (p *Parser) TableExpression() (TableExpression, bool, error) {
	te := TableExpression{}
	var err error
	var found bool

	te.FromClause, found, err = p.FromClause()
	if err != nil || !found {
		return te, found, err
	}

	te.WhereClause, err = p.WhereClause()
	if err != nil {
		return te, found, err
	}

	te.GroupByClause, err = p.GroupByClause()
	if err != nil {
		return te, found, err
	}

	return te, found, err
}

func (p *Parser) FromClause() (FromClause, bool, error) {
	fc := FromClause{}

	if !p.match(FROM) {
		return fc, false, nil
	}

	tn, err := p.TableName()
	if err != nil {
		return fc, true, err
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
			return fc, true, err
		}

		var rhs TableReference
		rhs, err = p.TableName()
		if err != nil {
			return fc, true, err
		}

		qj := QualifiedJoin{
			LHS:      tblRef,
			RHS:      rhs,
			JoinType: jt,
		}

		if err := p.requireMatch(ON); err != nil {
			return fc, true, err
		}

		var err error
		qj.JoinCondition, err = p.OrCondition()
		if err != nil {
			return fc, true, err
		}

		tblRef = qj
	}

	fc = append(fc, tblRef)

	return fc, true, nil
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

func (p *Parser) GroupByClause() ([]ColumnReference, error) {
	if !p.match(GROUP) {
		return nil, nil
	}
	if err := p.requireMatch(BY); err != nil {
		return nil, err
	}

	var ret []ColumnReference

	for {
		found, cr, err := p.ColumnReference()
		if err != nil {
			return ret, err
		}
		if !found {
			break
		}
		ret = append(ret, cr)
	}

	return ret, nil
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

	if pred, ok := pred.(ComparisonPredicate); ok {
		return Predicate{
			pred,
		}, nil
	}

	return pred, err
}

func (p *Parser) ComparisonPredicate() (interface{}, error) {
	var err error

	lhs, err := p.ValueExpression()
	if err != nil {
		return lhs, err
	}

	if !p.match(EQ, NEQ, LT, GT, LTE, GTE) {
		return lhs, nil
	}

	cp := ComparisonPredicate{
		LHS:    lhs,
		CompOp: p.Prev().Type,
	}

	cp.RHS, err = p.ValueExpression()
	if err != nil {
		return cp, err
	}

	return cp, nil
}

// ValueExpression is one of ColumnReference, integer literal, string literal, or nil
type ValueExpression any

func (p *Parser) ValueExpression() (ValueExpression, error) {
	if p.match(literals...) {
		return p.Prev().Val()
	}

	if ok, cr, err := p.ColumnReference(); err != nil {
		return nil, err
	} else if ok {
		return cr, nil
	}

	return nil, p.unexpectedTypeErr(literals...)
}

func (p *Parser) ColumnReference() (bool, ColumnReference, error) {
	ve := ColumnReference{}

	if !p.match(IDENT) {
		return false, ve, nil
	}

	if p.curType(DOT) {
		ve.Qualifier = p.Prev().Text
		p.Advance()
		if err := p.requireMatch(IDENT); err != nil {
			return false, ve, err
		}
	}

	ve.ColumnName = p.Prev().Text

	return true, ve, nil
}

func (p *Parser) SelectList() (SelectList, error) {
	sl := SelectList{}

	if p.match(ASTRSK) {
		sl = append(sl, DerivedColumn{
			ValueExpressionPrimary: Asterisk{},
		})
		return sl, nil
	}

	for {
		dc, err := p.DerivedColumn()
		if err != nil {
			return sl, err
		}
		if p.match(AS) {
			if p.Cur().Type != IDENT {
				return sl, p.requireMatch(IDENT)
			}
		}
		if p.match(IDENT) {
			dc.AsClause = p.Prev().Text
		}
		sl = append(sl, dc)
		if !p.match(COMMA) {
			break
		}
	}

	return sl, nil
}

func (p *Parser) DerivedColumn() (DerivedColumn, error) {
	dc := DerivedColumn{}

	found, setFunc, err := p.SetFunctionSpecification()
	if err != nil {
		return dc, err
	}
	if found {
		dc.ValueExpressionPrimary = setFunc
		return dc, err
	}

	dc.ValueExpressionPrimary, err = p.OrCondition()
	if err != nil {
		return dc, err
	}

	return dc, err
}

func (p *Parser) SetFunctionSpecification() (bool, any, error) {
	var setFunc any

	switch {
	case p.match(COUNT):
		if err := p.requireMatch(LPAREN); err != nil {
			return false, setFunc, err
		}
		count := Count{}
		foundColRef, ve, err := p.ColumnReference()
		if err != nil {
			return false, setFunc, err
		}
		if foundColRef {
			count.ValueExpression = ve
		} else if err := p.requireMatch(ASTRSK); err != nil {
			return false, setFunc, err
		}
		setFunc = count
		if err := p.requireMatch(RPAREN); err != nil {
			return false, setFunc, err
		}
	case p.match(AVG):
		if err := p.requireMatch(LPAREN); err != nil {
			return false, setFunc, err
		}
		foundColRef, ve, err := p.ColumnReference()
		if err != nil {
			return false, setFunc, err
		}
		if !foundColRef {
			return false, setFunc, errors.New("avg() requires a column argument")
		}
		setFunc = Average{
			ValueExpression: ve,
		}
		if err := p.requireMatch(RPAREN); err != nil {
			return false, setFunc, err
		}
	}

	return setFunc != nil, setFunc, nil
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

	var tvc TableValueConstructor
	for p.match(LPAREN) {
		var rvc RowValueConstructor

		for p.match(literals...) {
			val, err := p.Prev().Val()
			if err != nil {
				return is, err
			}
			rvc.RowValueConstructorList = append(rvc.RowValueConstructorList, val)
			if !p.match(COMMA) {
				break
			}
		}

		if err := p.requireMatch(RPAREN); err != nil {
			return is, err
		}

		tvc.TableValueConstructorList = append(tvc.TableValueConstructorList, rvc)

		if !p.match(COMMA) {
			break
		}
	}
	is.QueryExpression = tvc

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

func (p *Parser) Delete() (DeleteStatementSearched, error) {
	del := DeleteStatementSearched{}

	if err := p.requireMatch(FROM); err != nil {
		return del, err
	}

	if err := p.requireMatch(IDENT); err != nil {
		return del, err
	}

	del.TableName = p.Prev().Text

	var err error
	del.WhereClause, err = p.WhereClause()
	if err != nil {
		return del, err
	}

	return del, nil
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
	var unex string
	switch {
	case p.Cur().Type.IsLiteral():
		unex = fmt.Sprintf("`%s`", p.Cur().Text)
	default:
		unex = Tokens[p.Cur().Type]
	}
	var typeNames []string
	for _, tipe := range types {
		typeNames = append(typeNames, Tokens[tipe])
	}
	return fmt.Errorf("%w %s, expected %s", ErrUnexpectedToken, unex, strings.Join(typeNames, ", "))
}

func (p *Parser) requireInt() (int64, error) {
	if err := p.requireMatch(INT); err != nil {
		return 0, err
	}
	val, err := p.Prev().Val()
	return val.(int64), err
}
