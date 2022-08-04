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
	exec(`SELECT table_name, page_id FROM sys_pages`, t)
	exec(`SELECT table_name, field_name, field_length, field_type FROM sys_schema`, t)

	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (1, "John", "Doe")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (2, "Ikra", "Freeman")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (3, "Gerrard", "Torres")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (4, "Malia", "Brewer")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (5, "Willow", "Reeves")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (6, "Mylee", "Mclean")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (7, "Leland", "Booth")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (8, "Chance", "Snyder")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (9, "Cairo", "Lim")`, t)
	exec(`INSERT INTO people (person_id, first_name, last_name) VALUES (10, "Khadija", "Crane")`, t)

	exec(`SELECT person_id, first_name, last_name FROM people`, t)
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
	case sql.InsertStatement:
		if err := EvaluateInsert(stmt, "testdb"); err != nil {
			t.Errorf("error inserting into table: %s\n", err.Error())
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
