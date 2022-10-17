package engine

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/mkaminski/bkdb/btree"
	"github.com/mkaminski/bkdb/sql"
)

type Session struct {
	CurDB string
}

var (
	errDBNotSelected = errors.New("database not been selected")
	errDBNotExist    = errors.New("database does not exist")
	errDBExists      = errors.New("database already exists")
)

func (s *Session) Import() error {
	r := csv.NewReader(os.Stdin)

	i := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
			continue
		}

		name := record[0]
		var birth interface{}
		var death interface{}

		if record[1] != "NULL" {
			val, err := strconv.Atoi(record[1])
			if err != nil {
				fmt.Printf("failed to convert number. record: %v", record)
				continue
			}
			birth = int32(val)
		}
		if record[2] != "NULL" {
			val, err := strconv.Atoi(record[2])
			if err != nil {
				fmt.Printf("failed to convert number. record: %v", record)
				continue
			}
			death = int32(val)
		}

		q := sql.InsertStatement{
			TableName: "actor",
			InsertColumnsAndSource: sql.InsertColumnsAndSource{
				InsertColumnList: sql.InsertColumnList{
					ColumnNames: []string{
						"name",
						"birth",
						"death",
					},
				},
				QueryExpression: sql.TableValueConstructor{
					TableValueConstructorList: []sql.RowValueConstructor{
						{RowValueConstructorList: []interface{}{
							name,
							birth,
							death,
						}},
					},
				},
			},
		}

		if _, err := EvaluateInsert(q, "testdb"); err != nil {
			fmt.Printf("error inserting %v: %s\n", record, err.Error())
		} else if i%100 == 0 {
			fmt.Printf("inserted %d record(s) into %s\n", i, q.TableName)
		}

		i++
	}

	return nil
}

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
		if err := os.Mkdir("data", 0755); err != nil && !os.IsExist(err) {
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
		path, err := DBPath(s.CurDB)
		if err != nil {
			return err
		}
		rows, fields, err := EvaluateSelect(stmt, path, btree.NewFetcher())
		if err != nil {
			return err
		}
		printableFields := printableFields(stmt.SelectList, fields)
		printTable(printableFields, rows)
	case sql.InsertStatement:
		if count, err := EvaluateInsert(stmt, s.CurDB); err != nil {
			return err
		} else {
			fmt.Printf("inserted %d record(s) into %s\n", count, stmt.TableName)
		}
	case sql.UpdateStatementSearched:
		if err := EvaluateUpdate(stmt, s.CurDB, btree.NewFetcher()); err != nil {
			return err
		}
		fmt.Print("update successful\n", 1, stmt.TableName)
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
