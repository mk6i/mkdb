package engine

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateSelect(q sql.Select, db string) error {
	path, err := DBPath(db)
	if err != nil {
		return err
	}

	table := q.TableExpression.FromClause[0]

	rows, fields, err := nestedLoopJoin(path, table)
	if err != nil {
		return err
	}

	if q.TableExpression.WhereClause != nil {
		rows, err = filterRows(q.TableExpression.WhereClause.(sql.WhereClause), fields, rows)
		if err != nil {
			return err
		}
	}

	err = projectColumns(q.SelectList, fields, rows)
	if err != nil {
		return err
	}

	printableFields := printableFields(q.SelectList, fields)
	printTable(printableFields, rows)

	return nil
}

func nestedLoopJoin(path string, tf sql.TableReference) ([]*btree.Row, []*btree.Field, error) {

	switch v := tf.(type) {
	case string:
		tbl := string(v)
		r, f, err := btree.Select(path, tbl)
		if err != nil {
			return nil, nil, err
		}
		return r, f, err
	case sql.JoinedTable:
		switch v := v.(type) {
		case sql.QualifiedJoin:
			lRows, lFields, err := nestedLoopJoin(path, v.LHS)
			if err != nil {
				return nil, nil, err
			}
			rRows, rFields, err := nestedLoopJoin(path, v.RHS)
			if err != nil {
				return nil, nil, err
			}

			tmpRows := []*btree.Row{}

			tmpFields := []*btree.Field{}
			tmpFields = append(tmpFields, lFields...)
			tmpFields = append(tmpFields, rFields...)

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
			return tmpRows, tmpFields, nil
		}
	}

	return nil, nil, nil
}

func projectColumns(sl sql.SelectList, qfields []*btree.Field, rows []*btree.Row) error {
	if sl[0].ColumnName.Type == sql.ASTRSK {
		return nil
	}

	idxs := []int{}

	for _, selectField := range sl {
		for i, field := range qfields {
			if selectField.Qualifier == nil || selectField.Qualifier.(sql.Token).Text == field.Table {
				if selectField.ColumnName.Text == field.Column.(string) {
					idxs = append(idxs, i)
				}
			}
		}
	}

	for _, row := range rows {
		tmp := []interface{}{}
		for _, idx := range idxs {
			tmp = append(tmp, row.Vals[idx])
		}
		row.Vals = tmp
	}

	return nil
}

func filterRows(q sql.WhereClause, qfields []*btree.Field, rows []*btree.Row) ([]*btree.Row, error) {
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

func evaluate(q interface{}, qfields []*btree.Field, row *btree.Row) (bool, error) {
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

func evalOr(q sql.SearchCondition, qfields []*btree.Field, row *btree.Row) (bool, error) {
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

func evalAnd(q sql.BooleanTerm, qfields []*btree.Field, row *btree.Row) (bool, error) {
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

func evalComparisonPredicate(q sql.ComparisonPredicate, qfields []*btree.Field, row *btree.Row) (bool, error) {
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

func evalPrimary(q interface{}, qfields []*btree.Field, row *btree.Row) (interface{}, error) {
	switch t := q.(type) {
	case sql.ValueExpression:
		switch t.ColumnName.Type {
		case sql.STR:
			return t.ColumnName.Text, nil
		case sql.IDENT:
			for i, field := range qfields {
				if field.Column == t.ColumnName.Text {
					if t.Qualifier == nil || t.Qualifier.(sql.Token).Text == field.Table {
						return row.Vals[i], nil
					}
				}
			}
			return nil, fmt.Errorf("field not found: %s", t.ColumnName.Text)
		case sql.INT:
			intVal, err := strconv.Atoi(t.ColumnName.Text)
			if err != nil {
				return nil, err
			}
			return int32(intVal), nil
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

func printableFields(sl sql.SelectList, fields []*btree.Field) []string {
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
