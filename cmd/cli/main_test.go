package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/mkaminski/bkdb/engine"
	"github.com/mkaminski/bkdb/sql"
)

func TestMain(t *testing.T) {
	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			fmt.Printf("error removing db: %s", err.Error())
		}
	}()

	exec(`CREATE DATABASE testdb`)
	exec(`
		CREATE TABLE people (
			person_id int,
			first_name varchar(255),
			last_name varchar(255)
		)
	`)
}

func exec(q string) {
	stmt, err := parseSQL(q)
	if err != nil {
		fmt.Printf("error parsing sql: %s\n", err.Error())
		os.Exit(1)
	}

	switch stmt := stmt.(type) {
	case sql.CreateDatabase:
		if err := engine.EvaluateCreateDatabase(stmt); err != nil {
			fmt.Printf("error creating database: %s\n", err.Error())
			os.Exit(1)
		}
		fmt.Printf("successfully created database %s\n", stmt.Name)
	case sql.CreateTable:
		if err := engine.EvaluateCreateTable(stmt, "testdb"); err != nil {
			fmt.Printf("error creating table: %s\n", err.Error())
			os.Exit(1)
		}
		fmt.Printf("successfully created table %s\n", stmt.Name)
	default:
		panic("unsupported statement type")
	}
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
