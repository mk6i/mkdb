package engine

import (
	"os"
	"strings"
	"testing"

	"github.com/mkaminski/bkdb/sql"
)

func TestMain(t *testing.T) {

	if err := os.Mkdir("data", 0755); !os.IsExist(err) {
		t.Fatalf("error making data dir: %s", err.Error())
	}

	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			t.Logf("error removing db: %s", err.Error())
		}
	}()

	exec(`CREATE DATABASE testdb`, t)
	exec(`
		CREATE TABLE people (
			person_id int,
			first_name varchar(255),
			last_name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE cars (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE planes (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE boats (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE trains (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE helicopters (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE bicycles (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE motorcycles (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE minivans (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE scooters (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE rollerblades (
			name varchar(255)
		)
	`, t)
	exec(`
		CREATE TABLE tram (
			name varchar(255)
		)
	`, t)
	exec(`SELECT person_id, first_name, last_name FROM people`, t)
	exec(`SELECT table_name, page_id FROM sys_pages`, t)
	exec(`SELECT table_name, field_name, field_type, field_length FROM sys_schema`, t)
}

func exec(q string, t *testing.T) {
	stmt, err := parseSQL(q)
	if err != nil {
		t.Errorf("error parsing sql: %s\n", err.Error())
	}

	switch stmt := stmt.(type) {
	case sql.CreateDatabase:
		if err := EvaluateCreateDatabase(stmt); err != nil {
			t.Errorf("error creating database: %s\n", err.Error())
		}
		t.Logf("successfully created database %s\n", stmt.Name)
	case sql.CreateTable:
		if err := EvaluateCreateTable(stmt, "testdb"); err != nil {
			t.Errorf("error creating table: %s\n", err.Error())
		}
		t.Logf("successfully created table %s\n", stmt.Name)
	case sql.Select:
		if err := EvaluateSelect(stmt, "testdb"); err != nil {
			t.Errorf("error selecting table: %s\n", err.Error())
		}
	default:
		t.Errorf("unsupported statement type")
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
