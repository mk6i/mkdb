/*
Csvimport bulk loads CSV documents into mkdb.

Usage:

	csvimport [arguments]

The arguments are:

	-db (required)
		The destination database name where the destination table lives. The
		database must already exist before starting the import.

	-dest-cols (required)
		Comma-delimited list of table column names to populate from each CSV
		record. Each column name correponds to the column index at the same
		position in src-cols.

	-disable-wal-fsync (optional)
		If set, perform import with WAL fsync disabled. This can dramatically
		improve insert performance, with the risk of data loss if the import
		process crashes mid-operation. If this occurs, drop the database and table
		and start over.

	-separator (optional)
		Column separator character. By default use comma (,).

	-src-cols (required)
		Comma-delimited list of column indexes (0-indexed) to read from the CSV.

	-table (required)
		The destination table where rows are to be inserted. The table must
		already exist before starting the import.
*/
package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/mk6i/mkdb/engine"
	"github.com/mk6i/mkdb/sql"
	"github.com/mk6i/mkdb/storage"
)

var errMalformedRow = errors.New("error parsing row")

type importCfg struct {
	colTypes  []storage.DataType
	db        string
	dstCols   []string
	separator rune
	srcCols   []int
	table     string
}

var (
	cfgDb           = flag.String("db", "", "Destination DB name")
	cfgDestCols     = flag.String("dest-cols", "", "Destination table column names")
	cfgDisableFsync = flag.Bool("disable-wal-fsync", false, "Disable WAL fsync for performance (data loss is possible)")
	cfgSep          = flag.String("separator", ",", "CSV separator")
	cfgSrcCols      = flag.String("src-cols", "", "CSV source column indexes")
	cfgTable        = flag.String("table", "", "Destination table name")
)

func main() {
	flag.Parse()

	if flag.NFlag() == 0 {
		flag.PrintDefaults()
		return
	}

	rm, err := storage.OpenRelation(*cfgDb, !*cfgDisableFsync)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// create import configuration from the provided arguments
	cfg, err := makeConfig(rm)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	fmt.Printf("Import config: %+v\n", cfg)

	// start a background import job
	chOk, chErr := doBatchInsert(rm, cfg, os.Stdin)
	i := 0

	// report batch progress and errors as they arrive
	for {
		select {
		case _, ok := <-chOk:
			if ok {
				if i%100 == 0 {
					fmt.Printf("inserted %d record(s) into %s\n", i, cfg.table)
				}
				i++
			} else {
				chOk = nil
			}
		case err, ok := <-chErr:
			if ok {
				fmt.Println(err.Error())
			} else {
				chErr = nil
			}
		}
		if chOk == nil && chErr == nil {
			break
		}
	}
}

func makeConfig(rm engine.RelationManager) (importCfg, error) {
	cfg := importCfg{
		db:        *cfgDb,
		dstCols:   strings.Split(*cfgDestCols, ","),
		separator: []rune(*cfgSep)[0],
		table:     *cfgTable,
	}

	for _, col := range strings.Split(*cfgSrcCols, ",") {
		v, err := strconv.Atoi(col)
		if err != nil {
			return cfg, fmt.Errorf("err parsing indexes: %s", err.Error())
		}
		cfg.srcCols = append(cfg.srcCols, v)
	}

	var err error
	cfg.colTypes, err = colDataTypes(rm, cfg.table, cfg.dstCols)
	if err != nil {
		return cfg, fmt.Errorf("err getting column types: %s", err.Error())
	}

	return cfg, nil
}

// colDataTypes builds an array of data types for each destination column.
func colDataTypes(rm engine.RelationManager, table string, dstCols []string) ([]storage.DataType, error) {
	qs := fmt.Sprintf(`select field_name, field_type from sys_schema where table_name = '%s'`, table)
	ts := sql.NewTokenScanner(strings.NewReader(qs))
	tl := sql.TokenList{}

	for ts.Next() {
		tl.Add(ts.Cur())
	}

	p := sql.Parser{TokenList: tl}

	q, err := p.Parse()
	if err != nil {
		return nil, nil
	}

	rows, _, err := engine.EvaluateSelect(q.(sql.Select), rm)
	if err != nil {
		return nil, err
	}

	m := make(map[string]int64, len(rows))

	for _, row := range rows {
		m[row.Vals[0].(string)] = row.Vals[1].(int64)
	}

	tokens := make([]storage.DataType, len(dstCols))

	for i, col := range dstCols {
		fieldType, ok := m[col]
		if !ok {
			return nil, errors.New("didn't find column")
		}
		tokens[i] = storage.DataType(fieldType)
	}

	return tokens, nil
}

func newErrMalformedRow(msg string, line int, record []string) error {
	return fmt.Errorf("[line %d] %w: %s. contents: %v", line, errMalformedRow, msg, record)
}

// doBatchInsert starts an async routine that consumes the CSV and inserts each
// row into the destination table. Channel chOk sends each successful INSERT
// invent. Channel chErr sends each row failure.
func doBatchInsert(rm engine.RelationManager, cfg importCfg, r io.Reader) (chOk chan bool, chErr chan error) {
	csvRead := csv.NewReader(r)
	csvRead.ReuseRecord = true
	// allow a variable number of columns. error out row if a column specified
	// by cfg.srcCols is not present.
	csvRead.FieldsPerRecord = -1
	csvRead.Comma = cfg.separator

	q := sql.InsertStatement{
		TableName: cfg.table,
		InsertColumnsAndSource: sql.InsertColumnsAndSource{
			InsertColumnList: sql.InsertColumnList{
				ColumnNames: cfg.dstCols,
			},
		},
	}

	// find the largest column index in the source column list
	var maxCsvIdx int
	for _, i := range cfg.srcCols {
		if i > maxCsvIdx {
			maxCsvIdx = i
		}
	}

	chOk = make(chan bool)
	chErr = make(chan error)

	go func() {
		defer close(chErr)
		defer close(chOk)

		for line := 1; ; line++ {
			csvRow, err := csvRead.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				chErr <- newErrMalformedRow(err.Error(), line, csvRow)
				if _, ok := err.(*csv.ParseError); ok {
					continue
				}
				break // stop looping if it's an unexpected error
			}

			// make sure the CSV row is not missing any column indexes
			if maxCsvIdx >= len(csvRow) {
				chErr <- newErrMalformedRow(fmt.Sprintf("column %d not present", maxCsvIdx), line, csvRow)
				continue
			}

			sqlRow, err := csvToSql(cfg, csvRow)
			if err != nil {
				chErr <- newErrMalformedRow(err.Error(), line, csvRow)
				continue
			}

			q.InsertColumnsAndSource.QueryExpression = sql.TableValueConstructor{
				TableValueConstructorList: []sql.RowValueConstructor{
					{RowValueConstructorList: sqlRow},
				},
			}

			// execute the INSERT query
			if _, err := engine.EvaluateInsert(q, rm); err != nil {
				chErr <- err
			} else {
				chOk <- true
			}
		}
	}()

	return chOk, chErr
}

// csvToSql creates SQL INSERT values from each column in csvRow
func csvToSql(cfg importCfg, csvRow []string) ([]interface{}, error) {
	sqlRow := make([]interface{}, len(cfg.srcCols))

	for i, csvIdx := range cfg.srcCols {
		if csvRow[csvIdx] == "\\N" {
			sqlRow[i] = nil
			continue
		}
		switch cfg.colTypes[i] {
		case storage.TypeInt:
			val, err := strconv.Atoi(csvRow[csvIdx])
			if err != nil {
				return sqlRow, err
			}
			sqlRow[i] = int64(val)
		case storage.TypeBoolean:
			switch strings.ToLower(csvRow[csvIdx]) {
			case "1", "true", "t":
				sqlRow[i] = true
			case "0", "false", "f":
				sqlRow[i] = false
			default:
				return sqlRow, fmt.Errorf("unable to parse bool value `%s`", csvRow[csvIdx])
			}
		case storage.TypeVarchar:
			sqlRow[i] = csvRow[csvIdx]
		}
	}

	return sqlRow, nil
}
