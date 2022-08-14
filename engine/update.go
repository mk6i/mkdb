package engine

import (
	"fmt"
	"strconv"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

func EvaluateUpdate(q sql.UpdateStatementSearched, db string) error {

	path, err := DBPath(db)
	if err != nil {
		return err
	}

	table := q.TableName
	rows, fields, err := btree.Select(path, string(table), []*btree.Field{{Column: "*"}})
	if err != nil {
		return err
	}

	if q.Where != nil {
		rows, err = filterRows(q.Where.(sql.WhereClause), fields, rows)
		if err != nil {
			return err
		}
	}

	cols := []string{}
	updateSrc := []interface{}{}

	for _, set := range q.Set {
		cols = append(cols, set.ObjectColumn)
		t, err := castTokenVal(set.UpdateSource.ColumnName)
		if err != nil {
			return err
		}
		updateSrc = append(updateSrc, t)
	}

	for _, row := range rows {
		if err := btree.Update(path, q.TableName, row.RowID, cols, updateSrc); err != nil {
			return err
		}
	}

	return nil
}

func castTokenVal(t sql.Token) (interface{}, error) {
	switch t.Type {
	case sql.STR:
		return t.Text, nil
	case sql.INT:
		intVal, err := strconv.Atoi(t.Text)
		if err != nil {
			return nil, err
		}
		return int32(intVal), nil
	}
	return nil, fmt.Errorf("unsupported token type: %v", t)
}
