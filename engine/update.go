package engine

import (
	"fmt"

	"github.com/mk6i/mkdb/sql"
	"github.com/mk6i/mkdb/storage"
)

func EvaluateUpdate(q sql.UpdateStatementSearched, rm RelationManager) error {
	rm.StartTxn()
	defer rm.EndTxn()

	for _, set := range q.Set {
		if _, ok := set.UpdateSource.(sql.ColumnReference); ok {
			return fmt.Errorf("%w: can't set field value from another field", ErrTmpUnsupportedSyntax)
		}
	}

	table := q.TableName
	rows, fields, err := rm.Fetch(table)
	if err != nil {
		return err
	}

	if q.Where != nil {
		rows, err = filterRows(q.Where.(sql.WhereClause), fields, rows)
		if err != nil {
			return err
		}
	}

	var cols []string
	var updateSrc []interface{}

	for _, set := range q.Set {
		cols = append(cols, set.ObjectColumn)
		updateSrc = append(updateSrc, set.UpdateSource)
	}

	var batch storage.WALBatch
	for _, row := range rows {
		walEntries, err := rm.Update(q.TableName, row.RowID, cols, updateSrc)
		if err != nil {
			return err
		}
		batch = append(batch, walEntries...)
	}

	if err := rm.FlushWALBatch(batch); err != nil {
		return err
	}

	return nil
}
