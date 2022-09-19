package engine

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

var (
	ErrSortFieldNotFound = errors.New("sort field is not in select list")
)

func EvaluateSelect(q sql.Select, path string, fetcher btree.Fetcher) ([]*btree.Row, []*btree.Field, error) {

	table := q.TableExpression.FromClause[0]

	rows, fields, err := nestedLoopJoin(fetcher, path, table)
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

func nestedLoopJoin(fetcher btree.Fetcher, path string, tf sql.TableReference) ([]*btree.Row, btree.Fields, error) {

	switch v := tf.(type) {
	case sql.TableName:
		rows, fields, err := fetcher(path, v.Name)
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
			lRows, lFields, err := nestedLoopJoin(fetcher, path, v.LHS)
			if err != nil {
				return nil, nil, err
			}
			rRows, rFields, err := nestedLoopJoin(fetcher, path, v.RHS)
			if err != nil {
				return nil, nil, err
			}

			tmpRows := []*btree.Row{}

			tmpFields := btree.Fields{}
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
				rPadded := &btree.Row{Vals: make([]interface{}, len(rFields))}
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
				lPadded := &btree.Row{Vals: make([]interface{}, len(lFields))}
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

func projectColumns(sl sql.SelectList, qfields btree.Fields, rows []*btree.Row) (btree.Fields, error) {
	if sl[0].ColumnName.Type == sql.ASTRSK {
		return qfields, nil
	}

	idxs := []int{}
	var projFields btree.Fields
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
		tmp := []interface{}{}
		for _, idx := range idxs {
			tmp = append(tmp, row.Vals[idx])
		}
		row.Vals = tmp
	}

	return projFields, nil
}

func sortColumns(ssl []sql.SortSpecification, qfields btree.Fields, rows []*btree.Row) error {

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
			if errors.Is(err, btree.ErrFieldNotFound) {
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

func filterRows(q sql.WhereClause, qfields btree.Fields, rows []*btree.Row) ([]*btree.Row, error) {
	var ans []*btree.Row

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

func evaluate(q interface{}, qfields btree.Fields, row *btree.Row) (bool, error) {
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

func evalOr(q sql.SearchCondition, qfields btree.Fields, row *btree.Row) (bool, error) {
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

func evalAnd(q sql.BooleanTerm, qfields btree.Fields, row *btree.Row) (bool, error) {
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

func evalComparisonPredicate(q sql.ComparisonPredicate, qfields btree.Fields, row *btree.Row) (bool, error) {
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
	}

	return false, fmt.Errorf("nothing to compare here")
}

func evalPrimary(q interface{}, qfields btree.Fields, row *btree.Row) (interface{}, error) {
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

func printTable(selectList []string, rows []*btree.Row) {
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

func printableFields(sl sql.SelectList, fields btree.Fields) []string {
	ans := []string{}
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

func limit(limit int, rows []*btree.Row) []*btree.Row {
	if limit > len(rows) {
		return rows
	}
	return rows[0:limit]
}

func offset(offset int, rows []*btree.Row) []*btree.Row {
	if offset >= len(rows) {
		return []*btree.Row{}
	}
	return rows[offset:]
}
