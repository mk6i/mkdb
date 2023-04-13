package engine

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"
)

var (
	ErrIncompatTypeCompare  = errors.New("incompatible type comparison")
	ErrNonBoolJoinCond      = errors.New("non-boolean join condition")
	ErrSortFieldNotFound    = errors.New("sort field is not in select list")
	ErrTmpUnsupportedSyntax = errors.New("temporarily unsupported syntax")
)

func EvaluateSelect(q sql.Select, rm relationManager) ([]*storage.Row, []*storage.Field, error) {
	rm.StartTxn()
	defer rm.EndTxn()

	// handle case where no FROM clause is specified
	if len(q.TableExpression.FromClause) == 0 {
		var fields storage.Fields
		// create a placeholder row to populate
		rows := []*storage.Row{{}}
		fields, err := projectColumns(q.SelectList, fields, rows)
		return rows, fields, err
	}

	rows, fields, err := nestedLoopJoin(rm, q.TableExpression.FromClause[0])
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

	rows, err = aggregateRows(q.SelectList, q.GroupByClause, rows)
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
						result, err := evaluate(v.JoinCondition, tmpFields, tmpRow)
						if err != nil {
							return nil, nil, err
						}
						if doJoin, ok := result.(bool); !ok {
							return nil, nil, ErrNonBoolJoinCond
						} else if doJoin {
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
						result, err := evaluate(v.JoinCondition, tmpFields, tmpRow)
						if err != nil {
							return nil, nil, err
						}
						if doJoin, ok := result.(bool); !ok {
							return nil, nil, ErrNonBoolJoinCond
						} else if doJoin {
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
						result, err := evaluate(v.JoinCondition, tmpFields, tmpRow)
						if err != nil {
							return nil, nil, err
						}
						if doJoin, ok := result.(bool); !ok {
							return nil, nil, ErrNonBoolJoinCond
						} else if doJoin {
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

func projectColumns(selectList sql.SelectList, qfields storage.Fields, rows []*storage.Row) (storage.Fields, error) {
	var projFields storage.Fields

	lookup := map[sql.ColumnReference]int{}

	for _, elem := range selectList {
		switch elem := elem.ValueExpressionPrimary.(type) {
		case sql.Asterisk:
			return qfields, nil
		case sql.Average:
			var idx int
			var err error
			col := elem.ValueExpression.(sql.ColumnReference)
			if col.Qualifier != "" {
				idx, err = qfields.LookupColIdxByID(col.Qualifier, col.ColumnName)
			} else {
				idx, err = qfields.LookupFieldIdx(col.ColumnName)
			}
			if err != nil {
				return nil, err
			}
			lookup[col] = idx
			projFields = append(projFields, &storage.Field{
				Column: fmt.Sprintf("avg(%s)", col.String()),
			})
		case sql.Count:
			projFields = append(projFields, &storage.Field{
				Column: "count(*)",
			})
		case sql.ColumnReference:
			var idx int
			var err error
			if elem.Qualifier != "" {
				idx, err = qfields.LookupColIdxByID(elem.Qualifier, elem.ColumnName)
			} else {
				idx, err = qfields.LookupFieldIdx(elem.ColumnName)
			}
			if err != nil {
				return nil, err
			}
			lookup[elem] = idx
			projFields = append(projFields, qfields[idx])
		default:
			projFields = append(projFields, &storage.Field{
				Column: "?",
			})
		}

		// replace field name with alias
		if elem.AsClause != "" {
			projFields[len(projFields)-1].Column = elem.AsClause
		}
	}

	// rearrange columns according to order imposed by selectList
	// todo use copy to make more efficient?
	for _, row := range rows {
		var newVals []interface{}
		for _, elem := range selectList {
			switch elem := elem.ValueExpressionPrimary.(type) {
			case sql.Average:
				// add placeholder
				idx := lookup[elem.ValueExpression.(sql.ColumnReference)]
				newVals = append(newVals, row.Vals[idx].(int32))
			case sql.Count:
				// add placeholder
				newVals = append(newVals, int32(0))
			case sql.ColumnReference:
				idx := lookup[elem]
				newVals = append(newVals, row.Vals[idx])
			default:
				result, err := evaluate(elem, qfields, row)
				if err != nil {
					return nil, err
				}
				newVals = append(newVals, result)
			}

		}
		row.Vals = newVals
	}

	return projFields, nil
}

func aggregateRows(selectList sql.SelectList, groupBy []sql.ColumnReference, rows []*storage.Row) ([]*storage.Row, error) {
	if !selectList.HasAggrFunc() {
		return rows, nil
	}

	// if implicit group by with no results, return a single row that contains
	// 0-value results
	if len(groupBy) == 0 && len(rows) == 0 {
		return emptyAggregateRow(selectList, rows)
	}

	// map columns to indexes on the select list
	colToIdx := map[sql.ColumnReference]int{}
	for idx, col := range selectList {
		switch col := col.ValueExpressionPrimary.(type) {
		case sql.ColumnReference:
			colToIdx[col] = idx
		}
	}

	// generate keys for GROUP BY values
	groupKey := func(row *storage.Row) string {
		var key string
		for _, groupByCol := range groupBy {
			idx := colToIdx[groupByCol]
			key += fmt.Sprintf("%v", row.Vals[idx])
		}
		return key
	}

	// map group key to the index of the row that contains the aggregated value
	groupKeyToRow := map[string]int{}

	// this auxiliary data structure stores the counts per aggregated value
	// used in the cumulative average calculation
	counts := map[string]int32{}

	// calculate the aggregate values. de-dupe the rows by group key
	rowIdx := 0
	for _, row := range rows {
		key := groupKey(row)
		if _, ok := groupKeyToRow[key]; !ok {
			// this is the first time we encounter this unique group key. the
			// aggregate value for all subsequent rows that have this particular
			// group key is maintained in this row, and the rest of the rows
			// are discarded from the result set.
			groupKeyToRow[key] = rowIdx
			rows[rowIdx] = row
			rowIdx++
		}

		// the index of the de-duped row that maintains the aggregated value
		groupKeyIdx := groupKeyToRow[key]

		for colIdx, selectCol := range selectList {
			switch selectCol := selectCol.ValueExpressionPrimary.(type) {
			case sql.Count:
				rows[groupKeyIdx].Vals[colIdx] = rows[groupKeyIdx].Vals[colIdx].(int32) + 1
			case sql.Average:
				avgCol, ok := selectCol.ValueExpression.(sql.ColumnReference)
				if !ok {
					// should be caught in parser
					panic("avg() param must be a ColumnReference")
				}

				// update the count of this particular group key + value
				// combination
				countKey := fmt.Sprintf("%s%s", key, avgCol)
				if _, ok := counts[countKey]; !ok {
					counts[countKey] = 0
				}
				counts[countKey]++

				// calculate the cumulative average average
				avg := rows[groupKeyIdx].Vals[colIdx].(int32) * (counts[countKey] - 1)
				avg += row.Vals[colIdx].(int32)
				avg = int32(math.Round(float64(avg) / float64(counts[countKey])))

				// update the de-duped row with the recalculated average
				rows[groupKeyIdx].Vals[colIdx] = avg
			}
		}
	}

	// discard duplicate rows to avoid memory leaks
	for idx := rowIdx; idx < len(rows); idx++ {
		rows[idx] = nil
	}
	rows = rows[:rowIdx]

	return rows, nil
}

// emptyAggregateRow returns a row that represents an aggregation of an empty
// result set.
func emptyAggregateRow(selectList sql.SelectList, rows []*storage.Row) ([]*storage.Row, error) {
	row := &storage.Row{}
	for _, elem := range selectList {
		switch elem := elem.ValueExpressionPrimary.(type) {
		case sql.Count, sql.Average:
			row.Vals = append(row.Vals, int32(0))
		default:
			// handle any expressions in the select list
			result, err := evaluate(elem, storage.Fields{}, row)
			if err != nil {
				return rows, err
			}
			row.Vals = append(row.Vals, result)
		}

	}
	return []*storage.Row{row}, nil
}

func sortColumns(ssl []sql.SortSpecification, qfields storage.Fields, rows []*storage.Row) error {

	var sortIdxs []int
	for _, ss := range ssl {
		var idx int
		var err error
		if ss.SortKey.Qualifier != "" {
			idx, err = qfields.LookupColIdxByID(ss.SortKey.Qualifier, ss.SortKey.ColumnName)
		} else {
			idx, err = qfields.LookupFieldIdx(ss.SortKey.ColumnName)
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
			case bool:
				sortAsc = !lhs.(bool) && rhs.(bool)
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
		if val, ok := ok.(bool); val && ok {
			ans = append(ans, row)
		}
	}

	return ans, nil
}

func evaluate(q interface{}, qfields storage.Fields, row *storage.Row) (any, error) {
	switch v := q.(type) {
	case sql.SearchCondition: // or
		return evalOr(v, qfields, row)
	case sql.BooleanTerm: // and
		return evalAnd(v, qfields, row)
	case sql.Predicate:
		return evalComparisonPredicate(v.ComparisonPredicate, qfields, row)
	case int32, string, bool:
		return q, nil
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
	if lhs, ok := lhs.(bool); !ok {
		return false, newErrIncompatTypeCompare(lhs, rhs)
	} else if rhs, ok := rhs.(bool); !ok {
		return false, newErrIncompatTypeCompare(lhs, rhs)
	} else {
		return lhs || rhs, nil
	}
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
	if lhs, ok := lhs.(bool); !ok {
		return false, newErrIncompatTypeCompare(lhs, rhs)
	} else if rhs, ok := rhs.(bool); !ok {
		return false, newErrIncompatTypeCompare(lhs, rhs)
	} else {
		return lhs && rhs, nil
	}
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
	if t, ok := q.(sql.ColumnReference); ok {
		var idx int
		var err error
		if t.Qualifier != "" {
			idx, err = qfields.LookupColIdxByID(t.Qualifier, t.ColumnName)
		} else {
			idx, err = qfields.LookupFieldIdx(t.ColumnName)
		}
		if err != nil {
			return nil, err
		}
		return row.Vals[idx], nil
	}
	return q, nil
}

func printTable(rows []*storage.Row, fields []*storage.Field) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	fmt.Print("\n\r\n\r")

	for _, field := range fields {
		fmt.Fprintf(w, "| [%s]\t", field.Column)
	}

	fmt.Fprint(w, "|\n\r")

	for range fields {
		fmt.Fprint(w, "| --------------------\t")
	}

	fmt.Fprint(w, "|\n\r")

	for _, row := range rows {
		for _, elem := range row.Vals {
			fmt.Fprintf(w, "| %v\t", elem)
		}
		fmt.Fprint(w, "|\n\r")
	}

	w.Flush()

	fmt.Printf("\n\r%d result(s) returned\n\r", len(rows))
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
