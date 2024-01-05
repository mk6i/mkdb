package engine

import (
	"testing"

	"github.com/mk6i/mkdb/storage"
)

func TestIntegration(t *testing.T) {

	defer storage.ClearDataDir()

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
		`SELECT table_name, file_offset FROM sys_pages`,
		`SELECT table_name, field_name, field_length, field_type FROM sys_schema`,
		`INSERT INTO people (person_id, first_name) VALUES (1, 'John')`,
		`INSERT INTO people VALUES (2, 'Ikra', 'Freeman'),
			(3, 'Gerrard', 'Torres'),
			(4, 'Malia', 'Brewer'),
			(5, 'Willow', 'Reeves'),
			(6, 'Mylee', 'Mclean'),
			(7, 'Leland', 'Booth'),
			(8, 'Chance', 'Snyder'),
			(9, 'Cairo', 'Lim'),
			(10, 'Khadija', 'Crane')`,
		`SELECT person_id, first_name, last_name FROM people`,
		`SELECT * FROM people`,
		`SELECT * FROM people WHERE last_name = 'Brewer'`,
		`UPDATE people SET person_id = 600 WHERE last_name = 'Crane'`,
		`SELECT * FROM people WHERE last_name = 'Crane'`,
		`DELETE FROM people WHERE last_name = 'Crane'`,
		`SELECT * FROM people WHERE last_name = 'Crane'`,
	}

	s := Session{}
	for _, q := range queries {
		if err := s.ExecQuery(q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", q, err.Error())
		}
	}
}

func TestInsertNonExistentTable(t *testing.T) {

	defer storage.ClearDataDir()

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

	q := `INSERT INTO people (person_id, first_name, last_name) VALUES (1, 'John', 'Doe')`
	err := s.ExecQuery(q)

	if err != storage.ErrTableNotExist {
		t.Errorf("expected ErrTableNotExist error")
	}
}

func TestInsertColCountMismatch(t *testing.T) {

	defer storage.ClearDataDir()

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

	q := `INSERT INTO people (person_id, first_name, last_name) VALUES ('John', 'Doe')`
	err := s.ExecQuery(q)

	if err != storage.ErrColCountMismatch {
		t.Errorf("expected ErrColCountMismatch error")
	}
}

func TestInsertSansColListColCountMismatch(t *testing.T) {

	defer storage.ClearDataDir()

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

	q := `INSERT INTO people VALUES ('John', 'Doe')`
	err := s.ExecQuery(q)

	if err != storage.ErrColCountMismatch {
		t.Errorf("expected ErrColCountMismatch error")
	}
}

func TestSelectNonExistentTable(t *testing.T) {

	defer storage.ClearDataDir()

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

	if err != storage.ErrTableNotExist {
		t.Errorf("expected ErrTableNotExist error")
	}
}

func TestCreateDuplicateTable(t *testing.T) {

	defer storage.ClearDataDir()

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

	if err != storage.ErrTableAlreadyExist {
		t.Errorf("expected ErrTableAlreadyExist error")
	}
}
