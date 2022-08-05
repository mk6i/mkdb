package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

type DataType uint8

const (
	TYPE_INT = iota
	TYPE_VARCHAR
)

const (
	initalPageTableOffset    = 4096
	initialSchemaTableOffset = initalPageTableOffset * 2
	pageTableName            = "sys_pages"
	schemaTableName          = "sys_schema"
)

var (
	ErrTypeMismatch  = errors.New("types do not match")
	ErrTableNotExist = errors.New("table does not exist")
)

type FieldDef struct {
	DataType
	Name string
	Len  int32
}

func (f *FieldDef) Validate(val interface{}) error {
	switch f.DataType {
	case TYPE_INT:
		if reflect.TypeOf(val).Kind() != reflect.Int32 {
			return ErrTypeMismatch
		}
	case TYPE_VARCHAR:
		if reflect.TypeOf(val).Kind() != reflect.String {
			return ErrTypeMismatch
		}
	default:
		panic("unsupported validation type")
	}
	return nil
}

var pageTableSchema = Relation{
	Fields: []FieldDef{
		{
			Name:     "table_name",
			DataType: TYPE_VARCHAR,
			Len:      255,
		},
		{
			Name:     "page_id",
			DataType: TYPE_INT,
		},
	},
}

var schemaTableSchema = Relation{
	Fields: []FieldDef{
		{
			Name:     "table_name",
			DataType: TYPE_VARCHAR,
			Len:      255,
		},
		{
			Name:     "field_name",
			DataType: TYPE_VARCHAR,
			Len:      255,
		},
		{
			Name:     "field_type",
			DataType: TYPE_INT,
		},
		{
			Name:     "field_length",
			DataType: TYPE_INT,
			Len:      255,
		},
	},
}

type Relation struct {
	Fields []FieldDef
}

func (r *Relation) Encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	binary.Write(buf, binary.LittleEndian, uint16(len(r.Fields)))

	for _, fd := range r.Fields {
		if err := binary.Write(buf, binary.LittleEndian, uint8(fd.DataType)); err != nil {
			return buf, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(fd.Name))); err != nil {
			return buf, err
		}
		if err := binary.Write(buf, binary.LittleEndian, []byte(fd.Name)); err != nil {
			return buf, err
		}
		if fd.DataType == TYPE_VARCHAR {
			if err := binary.Write(buf, binary.LittleEndian, fd.Len); err != nil {
				return buf, err
			}
		}
	}
	return buf, nil
}

func (r *Relation) Decode(buf *bytes.Buffer) error {
	var len uint16
	binary.Read(buf, binary.LittleEndian, &len)

	for i := uint16(0); i < len; i++ {
		fd := FieldDef{}
		if err := binary.Read(buf, binary.LittleEndian, &fd.DataType); err != nil {
			return err
		}

		var len uint32
		if err := binary.Read(buf, binary.LittleEndian, &len); err != nil {
			return err
		}

		strBuf := make([]byte, len)
		_, err := buf.Read(strBuf)
		if err != nil {
			return err
		}

		fd.Name = string(strBuf)

		if fd.DataType == TYPE_VARCHAR {
			if err := binary.Read(buf, binary.LittleEndian, &fd.Len); err != nil {
				return err
			}
		}

		r.Fields = append(r.Fields, fd)
	}

	return nil
}

type Tuple struct {
	Vals     map[string]interface{}
	Relation *Relation
}

func (r *Tuple) Encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	for _, fd := range r.Relation.Fields {
		val := r.Vals[fd.Name]

		if err := fd.Validate(val); err != nil {
			return buf, err
		}

		switch fd.DataType {
		case TYPE_INT:
			if err := binary.Write(buf, binary.LittleEndian, val); err != nil {
				return buf, err
			}
		case TYPE_VARCHAR:
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(val.(string)))); err != nil {
				return buf, err
			}
			if err := binary.Write(buf, binary.LittleEndian, []byte(val.(string))); err != nil {
				return buf, err
			}
		}
	}

	return buf, nil
}

func (r *Tuple) Decode(buf *bytes.Buffer) error {
	for _, fd := range r.Relation.Fields {
		var v interface{}

		switch fd.DataType {
		case TYPE_INT:
			var val int32
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				return err
			}
			v = val
		case TYPE_VARCHAR:
			var len uint32
			if err := binary.Read(buf, binary.LittleEndian, &len); err != nil {
				return err
			}
			strBuf := make([]byte, len)
			_, err := buf.Read(strBuf)
			if err != nil {
				return err
			}
			v = string(strBuf)
		default:
			panic("unsupported data type")
		}
		r.Vals[fd.Name] = v
	}

	return nil
}

func CreateDB(path string) error {
	fs := &fileStore{
		path:           path,
		branchFactor:   4,
		nextFreeOffset: 4096,
	}
	if err := fs.save(); err != nil {
		return err
	}

	// create page table page
	pgTblPg, err := createPage(fs)
	if err != nil {
		return err
	}
	if pgTblPg.pageID != initalPageTableOffset {
		return fmt.Errorf("expected page table to be first page at offset %d, at offset %d instead", initalPageTableOffset, pgTblPg.pageID)
	}
	fs.setPageTableRoot(pgTblPg)
	if err := insertPageTable(fs, pgTblPg.pageID, pageTableName); err != nil {
		return err
	}

	// create schema table page
	schemaTblPg, err := createPage(fs)
	if err != nil {
		return err
	}
	if schemaTblPg.pageID != initialSchemaTableOffset {
		return fmt.Errorf("expected page table to be second page at offset %d, at offset %d instead", initialSchemaTableOffset, schemaTblPg.pageID)
	}
	if err := insertPageTable(fs, schemaTblPg.pageID, schemaTableName); err != nil {
		return err
	}

	if err := insertSchemaTable(fs, &pageTableSchema, pgTblPg.pageID, pageTableName); err != nil {
		return err
	}
	if err := insertSchemaTable(fs, &schemaTableSchema, schemaTblPg.pageID, schemaTableName); err != nil {
		return err
	}

	return nil
}

func CreateTable(path string, r *Relation, tableName string) error {
	fs := &fileStore{path: path}
	if err := fs.open(); err != nil {
		return err
	}

	pg, err := createPage(fs)
	if err != nil {
		return err
	}
	if err := insertPageTable(fs, pg.pageID, tableName); err != nil {
		return err
	}
	if err := insertSchemaTable(fs, r, pg.pageID, tableName); err != nil {
		return err
	}
	return nil
}

func createPage(fs *fileStore) (*page, error) {
	rootPg := &page{cellType: KeyValueCell}

	_, err := fs.append(rootPg)
	if err != nil {
		return rootPg, err
	}
	return rootPg, err
}

func insertPageTable(fs *fileStore, pageId uint32, tableName string) error {
	tuple := Tuple{
		Relation: &pageTableSchema,
		Vals: map[string]interface{}{
			"table_name": tableName,
			"page_id":    int32(pageId),
		},
	}

	buf, err := tuple.Encode()
	if err != nil {
		return err
	}

	// insert page table record
	pgTablePg, err := fs.fetch(fs.pageTableRoot)
	if err != nil {
		return err
	}

	bt := &BTree{store: fs}
	bt.setRoot(pgTablePg)

	id, err := bt.insert(buf.Bytes())
	if err != nil {
		return err
	}

	curRoot := bt.getRoot()
	if curRoot.pageID != pgTablePg.pageID {
		fs.setPageTableRoot(curRoot)
		if err := updatePageTable(fs, curRoot.pageID, tableName); err != nil {
			return err
		}
	}

	fmt.Printf("inserted new page table record for %s, page id: %d\n", tableName, id)

	return nil
}

func updatePageTable(fs *fileStore, pageId uint32, tableName string) error {
	pgTablePg, err := fs.fetch(fs.pageTableRoot)
	if err != nil {
		return err
	}

	bt := &BTree{store: fs}
	bt.setRoot(pgTablePg)

	ch := bt.scanRight()
	for cell := range ch {
		tuple := Tuple{
			Relation: &pageTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return err
		}
		if tuple.Vals["table_name"] == tableName {
			oldVal := tuple.Vals["page_id"]
			tuple.Vals["page_id"] = int32(pageId)
			buf, err := tuple.Encode()
			if err != nil {
				return err
			}
			cell.pg.updateCell(cell.key, buf.Bytes())
			fs.update(cell.pg)
			fmt.Printf("updated page table root from %d to %d, triggered by %s\n", oldVal, pageId, tableName)
			return nil
		}
	}

	return fmt.Errorf("unable to update page table entry for %d", pageId)
}

func insertSchemaTable(fs *fileStore, r *Relation, pageId uint32, tableName string) error {

	pgID, err := getRelationPageID(fs, schemaTableName)
	if err != nil {
		return err
	}

	schemaTablePg, err := fs.fetch(uint32(pgID))
	if err != nil {
		return err
	}

	bt := &BTree{store: fs}

	for _, fd := range r.Fields {
		tuple := Tuple{
			Relation: &schemaTableSchema,
			Vals: map[string]interface{}{
				"table_name":   tableName,
				"field_name":   fd.Name,
				"field_type":   int32(fd.DataType),
				"field_length": fd.Len,
			},
		}

		buf, err := tuple.Encode()
		if err != nil {
			return err
		}

		bt.setRoot(schemaTablePg)
		_, err = bt.insert(buf.Bytes())
		if err != nil {
			return err
		}

		newRootPg := bt.getRoot()
		if newRootPg.pageID != schemaTablePg.pageID {
			if err := updatePageTable(fs, newRootPg.pageID, schemaTableName); err != nil {
				return err
			}
			schemaTablePg = newRootPg
		}
	}
	return nil
}

func Select(path string, tableName string, fields []string) ([][]interface{}, []string, error) {

	fmt.Printf("Select query. Table: %s Fields: %s\n", tableName, fields)

	fs := &fileStore{path: path}
	if err := fs.open(); err != nil {
		return nil, nil, err
	}
	fmt.Printf("page table root offset: %d\n", fs.pageTableRoot)

	pageID, err := getRelationPageID(fs, tableName)
	if err != nil {
		return nil, nil, err
	}

	fmt.Printf("relation %s page id: %d\n", tableName, pageID)

	rs, err := getRelationSchema(fs, tableName)
	if err != nil {
		return nil, nil, err
	}

	if fields[0] == "*" {
		if len(fields) > 1 {
			return nil, nil, fmt.Errorf("right now select * may only contain one element")
		}
		fields = nil
		for _, fd := range rs.Fields {
			fields = append(fields, fd.Name)
		}
	}

	if err := validateFields(rs, fields); err != nil {
		return nil, nil, err
	}

	fmt.Printf("relation %s schema: %v\n", tableName, rs)

	rows, err := scanRelation(fs, uint32(pageID), rs, fields)

	return rows, fields, err
}

func getRelationPageID(fs *fileStore, relName string) (int32, error) {
	bt := BTree{store: fs}

	// retrieve page table
	pg, err := fs.fetch(fs.pageTableRoot)
	if err != nil {
		return 0, err
	}

	// todo why doesn't setRoot return an err?
	bt.setRoot(pg)

	ch := bt.scanRight()
	for cell := range ch {
		tuple := Tuple{
			Relation: &pageTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return 0, err
		}
		if tuple.Vals["table_name"] == relName {
			return tuple.Vals["page_id"].(int32), nil
		}
	}

	return 0, ErrTableNotExist
}

func getRelationSchema(fs *fileStore, relName string) (*Relation, error) {
	bt := BTree{store: fs}

	schemaTblOffset, err := getRelationPageID(fs, schemaTableName)
	if err != nil {
		return nil, err
	}

	// retrieve page table
	pg, err := fs.fetch(uint32(schemaTblOffset))
	if err != nil {
		return nil, err
	}

	// todo why doesn't setRoot return an err?
	bt.setRoot(pg)

	r := &Relation{}

	ch := bt.scanRight()
	for cell := range ch {
		tuple := Tuple{
			Relation: &schemaTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return nil, err
		}
		if tuple.Vals["table_name"] != relName {
			continue
		}
		r.Fields = append(r.Fields, FieldDef{
			Name:     tuple.Vals["field_name"].(string),
			Len:      tuple.Vals["field_length"].(int32),
			DataType: DataType(tuple.Vals["field_type"].(int32)),
		})
	}

	return r, nil
}

func scanRelation(fs *fileStore, pageID uint32, r *Relation, fields []string) ([][]interface{}, error) {

	var rows [][]interface{}

	bt := BTree{store: fs}

	// retrieve page table
	pg, err := fs.fetch(pageID)
	if err != nil {
		return nil, err
	}

	// todo why doesn't setRoot return an err?
	bt.setRoot(pg)

	ch := bt.scanRight()
	for cell := range ch {
		tuple := Tuple{
			Relation: r,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return nil, err
		}
		var row []interface{}
		for _, field := range fields {
			row = append(row, tuple.Vals[field])
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func validateFields(r *Relation, fields []string) error {
	allowed := make(map[string]bool, len(r.Fields))

	for _, fd := range r.Fields {
		allowed[fd.Name] = true
	}

	for _, field := range fields {
		if _, ok := allowed[field]; !ok {
			return fmt.Errorf("field %s does not exist", field)
		}
	}

	return nil
}

func Insert(path string, tableName string, cols []string, vals []interface{}) error {
	fs := &fileStore{path: path}
	if err := fs.open(); err != nil {
		return err
	}

	pgID, err := getRelationPageID(fs, tableName)
	if err != nil {
		return err
	}

	tablePg, err := fs.fetch(uint32(pgID))
	if err != nil {
		return err
	}

	schema, err := getRelationSchema(fs, tableName)
	if err != nil {
		return err
	}

	tuple := Tuple{
		Relation: schema,
		Vals:     make(map[string]interface{}, len(cols)),
	}

	for i, col := range cols {
		tuple.Vals[col] = vals[i]
	}

	buf, err := tuple.Encode()
	if err != nil {
		return err
	}

	bt := &BTree{store: fs}
	bt.setRoot(tablePg)
	_, err = bt.insert(buf.Bytes())
	if err != nil {
		return err
	}

	// update page table with new root if the old root split
	curPage := bt.getRoot()
	if curPage.pageID != tablePg.pageID {
		if err := updatePageTable(fs, curPage.pageID, tableName); err != nil {
			return err
		}
	}

	return nil
}
