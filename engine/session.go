package engine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/mk6i/mkdb/sql"
	"github.com/mk6i/mkdb/storage"
)

type Session struct {
	CurDB           string
	RelationService *storage.RelationService
}

type RelationManager interface {
	StartTxn()
	EndTxn()
	CreateTable(r *storage.Relation, tableName string) error
	MarkDeleted(tableName string, rowID uint32) (storage.WALBatch, error)
	Fetch(tableName string) ([]*storage.Row, []*storage.Field, error)
	Update(tableName string, rowID uint32, cols []string, updateSrc []interface{}) (storage.WALBatch, error)
	Insert(tableName string, cols []string, vals []interface{}) (storage.WALBatch, error)
	FlushWALBatch(batch storage.WALBatch) error
}

func (s *Session) Close() error {
	if s.RelationService != nil {
		return s.RelationService.Close()
	}
	return nil
}

func (s *Session) ExecQuery(q string) error {
	stmt, err := parseSQL(q)
	if err != nil {
		return fmt.Errorf("unable to parse sql: %s", err.Error())
	}

	switch stmt := stmt.(type) {
	case sql.CreateDatabase:
		if err := EvaluateCreateDatabase(stmt); err != nil {
			return err
		}
		fmt.Printf("created database %s\n\r", stmt.Name)
		return nil
	case sql.UseStatement:
		var err error
		s.CurDB = stmt.DBName
		s.RelationService, err = storage.OpenRelation(stmt.DBName, true)
		if err != nil {
			return err
		}
		fmt.Printf("selected database %s\n\r", stmt.DBName)
		return nil
	case sql.ShowDatabase:
		rows, fields, err := EvaluateShowDatabase(stmt)
		if err != nil {
			return err
		}
		printTable(rows, fields)
		return nil
	}

	if s.CurDB == "" {
		return errors.New("please select a database")
	}

	switch stmt := stmt.(type) {
	case sql.CreateTable:
		if err := EvaluateCreateTable(stmt, s.RelationService); err != nil {
			return err
		}
		fmt.Printf("created table %s\n\r", stmt.Name)
	case sql.Select:
		rows, fields, err := EvaluateSelect(stmt, s.RelationService)
		if err != nil {
			return err
		}
		printTable(rows, fields)
	case sql.InsertStatement:
		if count, err := EvaluateInsert(stmt, s.RelationService); err != nil {
			return err
		} else {
			fmt.Printf("inserted %d record(s) into %s\n\r", count, stmt.TableName)
		}
	case sql.UpdateStatementSearched:
		if err := EvaluateUpdate(stmt, s.RelationService); err != nil {
			return err
		}
		fmt.Printf("update successful %d %s\n\r", 1, stmt.TableName)
	case sql.DeleteStatementSearched:
		if count, err := EvaluateDelete(stmt, s.RelationService); err != nil {
			return err
		} else {
			fmt.Printf("deleted %d record(s) into %s\n\r", count, stmt.TableName)
		}
	default:
		return fmt.Errorf("unsupported statement type")
	}

	return nil
}

func parseSQL(q string) (interface{}, error) {
	ts := sql.NewTokenScanner(strings.NewReader(q))
	tl := sql.TokenList{}

	for ts.Next() {
		tl.Add(ts.Cur())
	}

	p := sql.Parser{TokenList: tl}

	return p.Parse()
}
