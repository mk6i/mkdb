package engine

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/mkaminski/bkdb/sql"
)

type Session struct {
	CurDB string
}

var (
	errDBNotSelected = errors.New("database not been selected")
	errDBNotExist    = errors.New("database does not exist")
	errAlreadyExists = errors.New("database already exists")
)

func (s *Session) ExecQuery(q string) error {
	stmt, err := parseSQL(q)
	if err != nil {
		return fmt.Errorf("error parsing sql: %s", err.Error())
	}

	switch stmt := stmt.(type) {
	case sql.UseStatement:
		_, err := DBPath(stmt.DBName)
		if err != nil {
			return err
		}
		s.CurDB = stmt.DBName
		fmt.Printf("selected database %s\n", stmt.DBName)
	case sql.CreateDatabase:
		if err := os.Mkdir("data", 0755); err != nil {
			if os.IsExist(err) {
				return errAlreadyExists
			}
			return fmt.Errorf("error making data dir: %s", err.Error())
		}
		if err := EvaluateCreateDatabase(stmt); err != nil {
			return err
		}
		fmt.Printf("created database %s\n", stmt.Name)
	case sql.CreateTable:
		if err := EvaluateCreateTable(stmt, s.CurDB); err != nil {
			return err
		}
		fmt.Printf("created table %s\n", stmt.Name)
	case sql.Select:
		if err := EvaluateSelect(stmt, s.CurDB); err != nil {
			return err
		}
	case sql.InsertStatement:
		if err := EvaluateInsert(stmt, s.CurDB); err != nil {
			return err
		}
		fmt.Printf("inserted %d record(s) into %s\n", 1, stmt.TableName)
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

func DBPath(db string) (string, error) {
	if db == "" {
		return "", errDBNotSelected
	}

	path := "data/" + strings.ToLower(db)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path, errDBNotExist
	}

	return path, nil
}
