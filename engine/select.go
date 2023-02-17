package engine

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

var (
	ErrSortFieldNotFound   = errors.New("sort field is not in select list")
	ErrIncompatTypeCompare = errors.New("incompatible type comparison")
)

func EvaluateSelect(q sql.Select, rm relationManager) ([]*storage.Row, []*storage.Field, error) {
	rm.StartTxn()
	defer rm.EndTxn()

	table := q.TableExpression.FromClause[0]

	rows, fields, err := nestedLoopJoin(rm, table)
	if err != nil {
		return nil, nil, err
	}

	if q.TableExpression.WhereClause != nil {
		rows, err = filterRows(q.TableExpression.WhereClause.(sql.WhereClause), fields, rows)
		if err != nil {
			return nil, nil, err
		}
	}

	fields, err = projectColumns(q.SelectList, fields, rows)
	if err != nil {
		return nil, nil, err
	}

	err = sortColumns(q.SortSpecificationList, fields, rows)
	if err != nil {
		return nil, nil, err
	}

	if q.LimitOffsetClause.OffsetActive {
		rows = offset(int(q.LimitOffsetClause.Offset), rows)
	}

	if q.LimitOffsetClause.LimitActive {
		rows = limit(int(q.LimitOffsetClause.Limit), rows)
	}

	return rows, fields, nil
}

func nestedLoopJoin(rm relationManager, tf sql.TableReference) ([]*storage.Row, storage.Fields, error) {

	switch v := tf.(type) {
	case sql.TableName:
		rows, fields, err := rm.Fetch(v.Name)
		if err != nil {
			return nil, nil, err
		}
		tableID := v.Name
		if v.CorrelationName != nil {
			tableID = v.CorrelationName.(string)
		}
		for _, fd := range fields {
			fd.TableID = tableID
		}
		return rows, fields, err
	case sql.JoinedTable:
		switch v := v.(type) {
		case sql.QualifiedJoin:
			lRows, lFields, err := nestedLoopJoin(rm, v.LHS)
			if err != nil {
				return nil, nil, err
			}
			rRows, rFields, err := nestedLoopJoin(rm, v.RHS)
			if err != nil {
				return nil, nil, err
			}

			var tmpRows []*storage.Row

			tmpFields := storage.Fields{}
			tmpFields = append(tmpFields, lFields...)
			tmpFields = append(tmpFields, rFields...)

			switch v.JoinType {
			case sql.INNER_JOIN:
				for _, lRow := range lRows {
					for _, rRow := range rRows {
						tmpRow := lRow.Merge(rRow)
						ok, err := evaluate(v.JoinCondition, tmpFields, tmpRow)
						if err != nil {
							return nil, nil, err
						}
						if ok {
							tmpRows = append(tmpRows, tmpRow)
						}
					}
				}
			case sql.LEFT_JOIN:
				rPadded := &storage.Row{Vals: make([]interface{}, len(rFields))}
				for _, lRow := range lRows {
					hasMatch := false
					for _, rRow := range rRows {
						tmpRow := lRow.Merge(rRow)
						ok, err := evaluate(v.JoinCondition, tmpFields, tmpRow)
						if err != nil {
							return nil, nil, err
						}
						if ok {
							hasMatch = true
							tmpRows = append(tmpRows, tmpRow)
						}
					}
					if !hasMatch {
						tmpRows = append(tmpRows, lRow.Merge(rPadded))
					}
				}
			case sql.RIGHT_JOIN:
				lPadded := &storage.Row{Vals: make([]interface{}, len(lFields))}
				for _, rRow := range rRows {
					hasMatch := false
					for _, lRow := range lRows {
						tmpRow := lRow.Merge(rRow)
						ok, err := evaluate(v.JoinCondition, tmpFields, tmpRow)
						if err != nil {
							return nil, nil, err
						}
						if ok {
							hasMatch = true
							tmpRows = append(tmpRows, tmpRow)
						}
					}
					if !hasMatch {
						tmpRows = append(tmpRows, lPadded.Merge(rRow))
					}
				}
			}
			return tmpRows, tmpFields, nil
		}
	}

	return nil, nil, nil
}

func projectColumns(sl sql.SelectList, qfields storage.Fields, rows []*storage.Row) (storage.Fields, error) {
	if sl[0].ColumnName.Type == sql.ASTRSK {
		return qfields, nil
	}

	var idxs []int
	var projFields storage.Fields
	for _, sf := range sl {
		var idx int
		var err error
		if sf.Qualifier != nil {
			idx, err = qfields.LookupColIdxByID(sf.Qualifier.(sql.Token).Text, sf.ColumnName.Text)
		} else {
			idx, err = qfields.LookupFieldIdx(sf.ColumnName.Text)
		}
		if err != nil {
			return nil, err
		}
		idxs = append(idxs, idx)
		projFields = append(projFields, qfields[idx])
	}

	for _, row := range rows {
		var tmp []interface{}
		for _, idx := range idxs {
			tmp = append(tmp, row.Vals[idx])
		}
		row.Vals = tmp
	}

	return projFields, nil
}

func sortColumns(ssl []sql.SortSpecification, qfields storage.Fields, rows []*storage.Row) error {

	var sortIdxs []int
	for _, ss := range ssl {
		var idx int
		var err error
		if ss.SortKey.Qualifier != nil {
			idx, err = qfields.LookupColIdxByID(ss.SortKey.Qualifier.(sql.Token).Text, ss.SortKey.ColumnName.Text)
		} else {
			idx, err = qfields.LookupFieldIdx(ss.SortKey.ColumnName.Text)
		}
		if err != nil {
			if errors.Is(err, storage.ErrFieldNotFound) {
				return fmt.Errorf("%w: %s", ErrSortFieldNotFound, err)
			}
			return err
		}
		sortIdxs = append(sortIdxs, idx)
	}

	sort.Slice(rows, func(i, j int) bool {
		for sortIdx, fieldIdx := range sortIdxs {
			lhs := rows[i].Vals[fieldIdx]
			rhs := rows[j].Vals[fieldIdx]

			if lhs == rhs {
				continue
			}

			sortAsc := false
			switch lhs.(type) {
			case int32:
				sortAsc = lhs.(int32) < rhs.(int32)
			case string:
				sortAsc = strings.Compare(lhs.(string), rhs.(string)) < 0
			default:
				panic(fmt.Sprintf("no comparison available for type %T", lhs))
			}
			if ssl[sortIdx].OrderingSpecification.Type == sql.DESC {
				sortAsc = !sortAsc
			}
			return sortAsc

		}

		// i & j are considered equal
		return false
	})

	return nil
}

func filterRows(q sql.WhereClause, qfields storage.Fields, rows []*storage.Row) ([]*storage.Row, error) {
	var ans []*storage.Row

	for _, row := range rows {
		ok, err := evaluate(q.SearchCondition, qfields, row)
		if err != nil {
			return nil, err
		}
		if ok {
			ans = append(ans, row)
		}
	}

	return ans, nil
}

func evaluate(q interface{}, qfields storage.Fields, row *storage.Row) (bool, error) {
	switch v := q.(type) {
	case sql.SearchCondition: // or
		return evalOr(v, qfields, row)
	case sql.BooleanTerm: // and
		return evalAnd(v, qfields, row)
	case sql.Predicate:
		return evalComparisonPredicate(v.ComparisonPredicate, qfields, row)
	}
	return false, fmt.Errorf("nothing to evaluate here")
}

func evalOr(q sql.SearchCondition, qfields storage.Fields, row *storage.Row) (bool, error) {
	lhs, err := evaluate(q.LHS, qfields, row)
	if err != nil {
		return false, err
	}
	rhs, err := evaluate(q.RHS, qfields, row)
	if err != nil {
		return false, err
	}
	return lhs || rhs, nil
}

func evalAnd(q sql.BooleanTerm, qfields storage.Fields, row *storage.Row) (bool, error) {
	lhs, err := evaluate(q.LHS, qfields, row)
	if err != nil {
		return false, err
	}
	rhs, err := evaluate(q.RHS, qfields, row)
	if err != nil {
		return false, err
	}
	return lhs && rhs, nil
}

func newErrIncompatTypeCompare(LHS any, RHS any) error {
	return fmt.Errorf("%w: cannot compare %v with %v", ErrIncompatTypeCompare, LHS, RHS)
}

func evalComparisonPredicate(q sql.ComparisonPredicate, qfields storage.Fields, row *storage.Row) (bool, error) {
	lhs, err := evalPrimary(q.LHS, qfields, row)
	if err != nil {
		return false, err
	}
	rhs, err := evalPrimary(q.RHS, qfields, row)
	if err != nil {
		return false, err
	}

	switch q.CompOp {
	case sql.EQ:
		return lhs == rhs, nil
	case sql.NEQ:
		return lhs != rhs, nil
	case sql.GT:
		switch lhs := lhs.(type) {
		case int32:
			if _rhs, ok := rhs.(int32); ok {
				return lhs > _rhs, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		case string:
			if rhs, ok := rhs.(string); ok {
				return strings.Compare(lhs, rhs) > 0, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		}
	case sql.GTE:
		switch lhs := lhs.(type) {
		case int32:
			if rhs, ok := rhs.(int32); ok {
				return lhs >= rhs, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		case string:
			if rhs, ok := rhs.(string); ok {
				return strings.Compare(lhs, rhs) >= 0, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		default:
			return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
		}
	case sql.LT:
		switch lhs := lhs.(type) {
		case int32:
			if rhs, ok := rhs.(int32); ok {
				return lhs < rhs, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		case string:
			if rhs, ok := rhs.(string); ok {
				return strings.Compare(lhs, rhs) < 0, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		}
	case sql.LTE:
		switch lhs := lhs.(type) {
		case int32:
			if rhs, ok := rhs.(int32); ok {
				return lhs <= rhs, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		case string:
			if rhs, ok := rhs.(string); ok {
				return strings.Compare(lhs, rhs) <= 0, nil
			} else {
				return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
			}
		default:
			return false, newErrIncompatTypeCompare(q.LHS, q.RHS)
		}
	}

	return false, fmt.Errorf("nothing to compare here")
}

func evalPrimary(q interface{}, qfields storage.Fields, row *storage.Row) (interface{}, error) {
	switch t := q.(type) {
	case sql.ValueExpression:
		switch t.ColumnName.Type {
		case sql.IDENT:
			var idx int
			var err error
			if t.Qualifier != nil {
				idx, err = qfields.LookupColIdxByID(t.Qualifier.(sql.Token).Text, t.ColumnName.Text)
			} else {
				idx, err = qfields.LookupFieldIdx(t.ColumnName.Text)
			}
			if err != nil {
				return nil, err
			}
			return row.Vals[idx], nil
		default:
			return t.ColumnName.Val()
		}
	}
	return nil, fmt.Errorf("nothing to compare here")
}

func printTable(selectList []string, rows []*storage.Row) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	fmt.Print("\n\n")

	for _, field := range selectList {
		fmt.Fprintf(w, "| [%s]\t", field)
	}

	fmt.Fprint(w, "|\n")

	for range selectList {
		fmt.Fprint(w, "| --------------------\t")
	}

	fmt.Fprint(w, "|\n")

	for _, row := range rows {
		for _, elem := range row.Vals {
			fmt.Fprintf(w, "| %v\t", elem)
		}
		fmt.Fprint(w, "|\n")
	}

	w.Flush()

	fmt.Printf("\n%d result(s) returned\n", len(rows))
}

func printableFields(sl sql.SelectList, fields storage.Fields) []string {
	var ans []string
	if sl[0].ColumnName.Type == sql.ASTRSK {
		for _, field := range fields {
			ans = append(ans, field.Column.(string))
		}
	} else {
		for _, field := range sl {
			if field.Qualifier != nil {
				ans = append(ans, fmt.Sprintf("%s.%s", field.Qualifier.(sql.Token).Text, field.ColumnName.Text))
			} else {
				ans = append(ans, field.ColumnName.Text)
			}
		}
	}
	return ans
}

func limit(limit int, rows []*storage.Row) []*storage.Row {
	if limit > len(rows) {
		return rows
	}
	return rows[0:limit]
}

func offset(offset int, rows []*storage.Row) []*storage.Row {
	if offset >= len(rows) {
		return []*storage.Row{}
	}
	return rows[offset:]
}
