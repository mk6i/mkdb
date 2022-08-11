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

	var fields []string

	for _, elem := range q.SelectList {
		if elem.ColumnName.Type == sql.ASTRSK {
			fields = append(fields, "*")
		} else {
			fields = append(fields, elem.ColumnName.Text)
		}
	}

	table := q.TableExpression.FromClause[0]

	rows, fields, err := btree.Select(path, table.(string), fields)
	if err != nil {
		return err
	}

	if q.TableExpression.WhereClause != nil {
		rows, err = filterRows(q.TableExpression.WhereClause.(sql.WhereClause), fields, rows)
		if err != nil {
			return err
		}
	}

	printTable(fields, rows)

	return nil
}

func filterRows(q sql.WhereClause, qfields []string, rows []*btree.Row) ([]*btree.Row, error) {
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

func evaluate(q interface{}, qfields []string, row *btree.Row) (bool, error) {
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

func evalOr(q sql.SearchCondition, qfields []string, row *btree.Row) (bool, error) {
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

func evalAnd(q sql.BooleanTerm, qfields []string, row *btree.Row) (bool, error) {
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

func evalComparisonPredicate(q sql.ComparisonPredicate, qfields []string, row *btree.Row) (bool, error) {
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

func evalPrimary(q interface{}, qfields []string, row *btree.Row) (interface{}, error) {
	switch t := q.(type) {
	case sql.ValueExpression:
		switch t.ColumnName.Type {
		case sql.STR:
			return t.ColumnName.Text, nil
		case sql.IDENT:
			for i, field := range qfields {
				if field == t.ColumnName.Text {
					return row.Vals[i], nil
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

func printTable(fields []string, rows []*btree.Row) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	fmt.Print("\n\n")

	for _, field := range fields {
		fmt.Fprintf(w, "| [%v]\t", field)
	}

	fmt.Fprint(w, "|\n")

	for range fields {
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
