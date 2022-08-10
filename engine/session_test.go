package engine

import (
	"os"
	"testing"

	"github.com/mkaminski/bkdb/btree"
)

func TestMain(t *testing.T) {

	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			t.Logf("error removing db: %s", err.Error())
		}
	}()

	queries := []string{
		`CREATE DATABASE testdb`,
		`USE testdb`,
		`CREATE TABLE people (
			person_id int,
			first_name varchar(255),
			last_name varchar(255)
		)`,
		`CREATE TABLE cars (
			name varchar(255)
		)`,
		`CREATE TABLE planes (
			name varchar(255)
		)`,
		`CREATE TABLE boats (
			name varchar(255)
		)`,
		`CREATE TABLE trains (
			name varchar(255)
		)`,
		`CREATE TABLE helicopters (
			name varchar(255)
		)`,
		`CREATE TABLE bicycles (
			name varchar(255)
		)`,
		`CREATE TABLE motorcycles (
			name varchar(255)
		)`,
		`CREATE TABLE minivans (
			name varchar(255)
		)`,
		`CREATE TABLE scooters (
			name varchar(255)
		)`,
		`CREATE TABLE rollerblades (
			name varchar(255)
		)`,
		`CREATE TABLE tram (
			name varchar(255)
		)`,
		`SELECT table_name, page_id FROM sys_pages`,
		`SELECT table_name, field_name, field_length, field_type FROM sys_schema`,
		`INSERT INTO people (person_id, first_name) VALUES (1, "John")`,
		`INSERT INTO people VALUES (2, "Ikra", "Freeman")`,
		`INSERT INTO people VALUES (3, "Gerrard", "Torres")`,
		`INSERT INTO people VALUES (4, "Malia", "Brewer")`,
		`INSERT INTO people VALUES (5, "Willow", "Reeves")`,
		`INSERT INTO people VALUES (6, "Mylee", "Mclean")`,
		`INSERT INTO people VALUES (7, "Leland", "Booth")`,
		`INSERT INTO people VALUES (8, "Chance", "Snyder")`,
		`INSERT INTO people VALUES (9, "Cairo", "Lim")`,
		`INSERT INTO people VALUES (10, "Khadija", "Crane")`,
		`SELECT person_id, first_name, last_name FROM people`,
		`SELECT * FROM people`,
		`SELECT * FROM people WHERE last_name = "Brewer"`,
		`UPDATE people SET person_id = 600 WHERE last_name = "Crane"`,
		`SELECT * FROM people WHERE last_name = "Crane"`,
	}

	s := Session{}
	for _, q := range queries {
		if err := s.ExecQuery(q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", q, err.Error())
		}
	}
}

func TestInsertNonExistentTable(t *testing.T) {

	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			t.Logf("error removing db: %s", err.Error())
		}
	}()

	s := Session{}

	queries := []string{
		`CREATE DATABASE testdb`,
		`USE testdb`,
	}
	for _, q := range queries {
		if err := s.ExecQuery(q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", q, err.Error())
		}
	}

	q := `INSERT INTO people (person_id, first_name, last_name) VALUES (1, "John", "Doe")`
	err := s.ExecQuery(q)

	if err != btree.ErrTableNotExist {
		t.Errorf("expected ErrTableNotExist error")
	}
}

func TestInsertColCountMismatch(t *testing.T) {

	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			t.Logf("error removing db: %s", err.Error())
		}
	}()

	s := Session{}

	queries := []string{
		`CREATE DATABASE testdb`,
		`USE testdb`,
		`CREATE TABLE people (
			person_id int,
			first_name varchar(255),
			last_name varchar(255)
		)`,
	}
	for _, q := range queries {
		if err := s.ExecQuery(q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", q, err.Error())
		}
	}

	q := `INSERT INTO people (person_id, first_name, last_name) VALUES ("John", "Doe")`
	err := s.ExecQuery(q)

	if err != btree.ErrColCountMismatch {
		t.Errorf("expected ErrColCountMismatch error")
	}
}

func TestInsertSansColListColCountMismatch(t *testing.T) {

	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			t.Logf("error removing db: %s", err.Error())
		}
	}()

	s := Session{}

	queries := []string{
		`CREATE DATABASE testdb`,
		`USE testdb`,
		`CREATE TABLE people (
			person_id int,
			first_name varchar(255),
			last_name varchar(255)
		)`,
	}
	for _, q := range queries {
		if err := s.ExecQuery(q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", q, err.Error())
		}
	}

	q := `INSERT INTO people VALUES ("John", "Doe")`
	err := s.ExecQuery(q)

	if err != btree.ErrColCountMismatch {
		t.Errorf("expected ErrColCountMismatch error")
	}
}

func TestSelectNonExistentTable(t *testing.T) {

	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			t.Logf("error removing db: %s", err.Error())
		}
	}()

	s := Session{}

	queries := []string{
		`CREATE DATABASE testdb`,
		`USE testdb`,
	}
	for _, q := range queries {
		if err := s.ExecQuery(q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", q, err.Error())
		}
	}

	q := `SELECT person_id, first_name, last_name FROM people`
	err := s.ExecQuery(q)

	if err != btree.ErrTableNotExist {
		t.Errorf("expected ErrTableNotExist error")
	}
}

func TestCreateDuplicateTable(t *testing.T) {

	defer func() {
		if err := os.Remove("data/testdb"); err != nil {
			t.Logf("error removing db: %s", err.Error())
		}
	}()

	s := Session{}

	queries := []string{
		`CREATE DATABASE testdb`,
		`USE testdb`,
		`CREATE TABLE motorcycles (name varchar(255))`,
	}
	for _, q := range queries {
		if err := s.ExecQuery(q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", q, err.Error())
		}
	}

	q := `CREATE TABLE motorcycles (name varchar(255))`
	err := s.ExecQuery(q)

	if err != btree.ErrTableAlreadyExist {
		t.Errorf("expected ErrTableAlreadyExist error")
	}
}
