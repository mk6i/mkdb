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
		if elem.Token.Type == sql.ASTRSK {
			fields = append(fields, "*")
		} else {
			fields = append(fields, elem.Token.Text)
		}
	}

	table := q.TableExpression.FromClause[0]

	rows, fields, err := btree.Select(path, string(table), fields)
	if err != nil {
		return err
	}

	rows, err = filterRows(q, fields, rows)
	if err != nil {
		return err
	}

	printTable(fields, rows)

	return nil
}

func filterRows(q sql.Select, qfields []string, rows [][]interface{}) ([][]interface{}, error) {
	if q.TableExpression.WhereClause == nil {
		// don't do anything if there's no WHERE clause
		return rows, nil
	}

	sc := q.TableExpression.WhereClause.(sql.WhereClause).SearchCondition
	var ans [][]interface{}

	for _, row := range rows {
		ok, err := evaluate(sc, qfields, row)
		if err != nil {
			return nil, err
		}
		if ok {
			ans = append(ans, row)
		}
	}

	return ans, nil
}

func evaluate(q interface{}, qfields []string, row []interface{}) (bool, error) {
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

func evalOr(q sql.SearchCondition, qfields []string, row []interface{}) (bool, error) {
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

func evalAnd(q sql.BooleanTerm, qfields []string, row []interface{}) (bool, error) {
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

func evalComparisonPredicate(q sql.ComparisonPredicate, qfields []string, row []interface{}) (bool, error) {
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

func evalPrimary(q interface{}, qfields []string, row []interface{}) (interface{}, error) {
	switch t := q.(type) {
	case sql.ValueExpression:
		switch t.Token.Type {
		case sql.STR:
			return t.Token.Text, nil
		case sql.IDENT:
			for i, field := range qfields {
				if field == t.Token.Text {
					return row[i], nil
				}
			}
			return nil, fmt.Errorf("field not found: %s", t.Token.Text)
		case sql.INT:
			intVal, err := strconv.Atoi(t.Token.Text)
			if err != nil {
				return nil, err
			}
			return int32(intVal), nil
		}
	}
	return nil, fmt.Errorf("nothing to compare here")
}

func printTable(fields []string, rows [][]interface{}) {
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
		for _, elem := range row {
			fmt.Fprintf(w, "| %v\t", elem)
		}
		fmt.Fprint(w, "|\n")
	}

	w.Flush()

	fmt.Printf("\n%d result(s) returned\n", len(rows))
}
