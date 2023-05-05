package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
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
	srcCols []int
	dstCols []string
	table   string
	db      string
}

func init() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:8080", nil))
	}()
}

// usage ./copy -src-cols '1,2,3' -dest-cols 'name,birth,death' -table actor -db
func main() {

	var cfg importCfg

	srcCols := flag.String("src-cols", "", "CSV source column indexes")
	dstCols := flag.String("dest-cols", "", "DB destination columns")
	table := flag.String("table", "", "Destination table name")
	db := flag.String("db", "", "Destination DB name")

	flag.Parse()

	if flag.NFlag() == 0 {
		flag.PrintDefaults()
		return
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
	cfg.dstCols = strings.Split(*dstCols, ",")
	cfg.table = *table
	cfg.db = *db

	fmt.Printf("srcCols: %v, dstCols: %v, table: %s, db: %s\n", cfg.srcCols, cfg.dstCols, cfg.table, cfg.db)

	sess := &engine.Session{}

	defer sess.Close()

	q := fmt.Sprintf("USE %s", cfg.db)
	if err := sess.ExecQuery(q); err != nil {
		fmt.Printf("failed to import: %s\n", err.Error())
		return
	}

	if err := Import(sess, cfg); err != nil {
		fmt.Printf("failed to import: %s\n", err.Error())
	}
}

func getDataTypes(s *engine.Session, cfg importCfg) ([]storage.DataType, error) {

	qs := fmt.Sprintf(`select field_name, field_type from sys_schema where table_name = '%s'`, cfg.table)
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

	rows, _, err := engine.EvaluateSelect(q.(sql.Select), s.RelationService)
	if err != nil {
		return nil, err
	}

	m := make(map[string]int32, len(rows))

	for _, row := range rows {
		m[row.Vals[0].(string)] = row.Vals[1].(int32)
	}

	tokens := make([]storage.DataType, len(cfg.dstCols))

	for i, col := range cfg.dstCols {
		fieldType, ok := m[col]
		if !ok {
			return nil, errors.New("didn't find column")
		}
		tokens[i] = storage.DataType(fieldType)
	}

	return tokens, nil
}

func Import(s *engine.Session, cfg importCfg) error {
	f, err := os.Open("/Users/mike/Downloads/name.basics.tsv")
	if err != nil {
		return err
	}
	// r := csv.NewReader(os.Stdin)
	r := csv.NewReader(f)
	r.Comma = '\t'

	q := sql.InsertStatement{
		TableName: cfg.table,
		InsertColumnsAndSource: sql.InsertColumnsAndSource{
			InsertColumnList: sql.InsertColumnList{
				ColumnNames: cfg.dstCols,
			},
		},
	}

	types, err := getDataTypes(s, cfg)
	if err != nil {
		return err
	}

	ch := make(chan sql.InsertStatement)

	go func() {
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Println(err)
				continue
			}

			row := make([]interface{}, len(cfg.srcCols))

			for i, csvIdx := range cfg.srcCols {
				if record[csvIdx] == "\\N" {
					row[i] = nil
					continue
				}
				switch types[i] {
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
					row[i] = strings.ReplaceAll(record[csvIdx], "'", "\\'")
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

	return s.BulkInsert(ch)
}
