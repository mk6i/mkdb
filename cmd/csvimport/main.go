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

	"github.com/mkaminski/bkdb/engine"
	"github.com/mkaminski/bkdb/sql"
	"github.com/mkaminski/bkdb/storage"

	"net/http"
	_ "net/http/pprof"
)

type importCfg struct {
	srcCols   []int
	dstCols   []string
	dataTypes []storage.DataType
	table     string
	db        string
	separator rune
}

func init() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:8080", nil))
	}()
}

// usage ./copy -src-cols '1,2,3' -dest-cols 'name,birth,death' -table actor -db
func main() {
	sess := &engine.Session{}

	srcCols := flag.String("src-cols", "", "CSV source column indexes")
	dstCols := flag.String("dest-cols", "", "DB destination columns")
	separator := flag.String("separator", ",", "CSV separator")
	table := flag.String("table", "", "Destination table name")
	db := flag.String("db", "", "Destination DB name")

	flag.Parse()

	if flag.NFlag() == 0 {
		flag.PrintDefaults()
		return
	}

	cfg := importCfg{
		db:        *db,
		separator: []rune(*separator)[0],
		table:     *table,
		dstCols:   strings.Split(*dstCols, ","),
	}

	for _, col := range strings.Split(*srcCols, ",") {
		v, err := strconv.Atoi(col)
		if err != nil {
			fmt.Printf("err parsing indexes: %s", err.Error())
			os.Exit(1)
			return
		}
		cfg.srcCols = append(cfg.srcCols, v)
	}

	fmt.Printf("srcCols: %v, dstCols: %v, table: %s, db: %s\n", cfg.srcCols, cfg.dstCols, cfg.table, cfg.db)

	defer sess.Close()

	q := fmt.Sprintf("USE %s", cfg.db)
	if err := sess.ExecQuery(q); err != nil {
		fmt.Printf("failed to import: %s\n", err.Error())
		return
	}

	var err error
	cfg.dataTypes, err = getDataTypes(sess.RelationService, cfg.table, cfg.dstCols)
	if err != nil {
		fmt.Printf("err parsing indexes: %s", err.Error())
		os.Exit(1)
		return
	}

	if err := CSVImport(sess.RelationService, cfg, os.Stdin); err != nil {
		fmt.Printf("failed to import: %s\n", err.Error())
	}
}

func getDataTypes(rm engine.RelationManager, table string, dstCols []string) ([]storage.DataType, error) {

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

	m := make(map[string]int32, len(rows))

	for _, row := range rows {
		m[row.Vals[0].(string)] = row.Vals[1].(int32)
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

func CSVImport(rm engine.RelationManager, cfg importCfg, r io.Reader) error {
	csvRead := csv.NewReader(r)
	csvRead.Comma = '\t'
	csvRead.LazyQuotes = true
	csvRead.ReuseRecord = true
	csvRead.FieldsPerRecord = -1
	// csvRead.Comma = cfg.separator

	q := sql.InsertStatement{
		TableName: cfg.table,
		InsertColumnsAndSource: sql.InsertColumnsAndSource{
			InsertColumnList: sql.InsertColumnList{
				ColumnNames: cfg.dstCols,
			},
		},
	}

	var maxCsvIdx int
	for _, i := range cfg.srcCols {
		if i > maxCsvIdx {
			maxCsvIdx = i
		}
	}

	ch := make(chan sql.InsertStatement)

	go func() {
		for {
			record, err := csvRead.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				if _, ok := err.(*csv.ParseError); ok {
					fmt.Printf("error parsing csv: %s", err.Error())
					continue
				}
				fmt.Printf("fatal error parsing csv: %s", err.Error())
				break
			}

			if maxCsvIdx >= len(record) {
				fmt.Printf("insufficient col count in csv record: %v\n", record)
				continue
			}

			row := make([]interface{}, len(cfg.srcCols))

			for i, csvIdx := range cfg.srcCols {
				if record[csvIdx] == "\\N" {
					row[i] = nil
					continue
				}
				switch cfg.dataTypes[i] {
				case storage.TypeInt:
					val, err := strconv.Atoi(record[csvIdx])
					if err != nil {
						fmt.Printf("failed to convert number. record: %v", record)
						continue
					}
					row[i] = int32(val)
				case storage.TypeBoolean:
					panic("no bools")
				case storage.TypeVarchar:
					row[i] = record[csvIdx]
				}
			}

			q.InsertColumnsAndSource.QueryExpression = sql.TableValueConstructor{
				TableValueConstructorList: []sql.RowValueConstructor{
					{RowValueConstructorList: row},
				},
			}

			ch <- q
		}

		close(ch)
	}()

	i := 0
	for q := range ch {
		if _, err := engine.EvaluateInsert(q, rm); err != nil {
			fmt.Printf("error inserting %v: %s\n", q, err.Error())
		} else if i%100 == 0 {
			fmt.Printf("inserted %d record(s) into %s\n", i, q.TableName)
		}
		i++
	}

	return nil
}
