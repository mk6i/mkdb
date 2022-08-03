package engine

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateSelect(q sql.Select, db string) error {
	path := "data/" + strings.ToLower(db)

	var fields []string

	for _, elem := range q.SelectList {
		fields = append(fields, elem.Token.Text)
	}

	table := q.TableExpression.FromClause[0]

	rows, err := btree.Select(path, string(table), fields)
	if err != nil {
		return err
	}

	printTable(fields, rows)

	return nil
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

	fmt.Printf("\n%d result(s) returned\n\n\n", len(rows))
}
