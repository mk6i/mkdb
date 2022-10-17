package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type DataType uint8

const (
	TYPE_INT = iota
	TYPE_VARCHAR
)

const (
	initalPageTableOffset    = pageSize
	initialSchemaTableOffset = initalPageTableOffset * 2
	pageTableName            = "sys_pages"
	schemaTableName          = "sys_schema"
)

var (
	ErrTypeMismatch      = errors.New("types do not match")
	ErrTableNotExist     = errors.New("table does not exist")
	ErrTableAlreadyExist = errors.New("table already exists")
	ErrColCountMismatch  = errors.New("value list count does not match column list count")
	ErrFieldAmbiguous    = errors.New("field is ambiguous")
	ErrFieldNotFound     = errors.New("field not found")
)

type Fetcher func(path string, tableName string) ([]*Row, []*Field, error)

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
		isNull := val == nil

		if err := binary.Write(buf, binary.LittleEndian, isNull); err != nil {
			return buf, err
		}

		if isNull {
			continue
		}

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
		var isNull bool
		if err := binary.Read(buf, binary.LittleEndian, &isNull); err != nil {
			return err
		}
		if isNull {
			continue
		}

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
		nextFreeOffset: pageSize,
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
	if err := fs.setPageTableRoot(pgTblPg); err != nil {
		return err
	}
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

	_, err := getRelationPageID(fs, tableName)
	if err != ErrTableNotExist {
		return ErrTableAlreadyExist
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

	if err := fs.append(rootPg); err != nil {
		return rootPg, err
	}
	return rootPg, nil
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

	curRoot, err := bt.getRoot()
	if err != nil {
		return err
	}

	if curRoot.pageID != pgTablePg.pageID {
		fs.setPageTableRoot(curRoot)
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

	found := false
	err = bt.scanRight(func(cell *keyValueCell) (bool, error) {
		tuple := Tuple{
			Relation: &pageTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return StopScanning, err
		}
		if tuple.Vals["table_name"] == tableName {
			oldVal := tuple.Vals["page_id"]
			tuple.Vals["page_id"] = int32(pageId)
			buf, err := tuple.Encode()
			if err != nil {
				return StopScanning, err
			}
			if err := cell.pg.updateCell(cell.key, buf.Bytes()); err != nil {
				return StopScanning, err
			}
			if err := fs.update(cell.pg); err != nil {
				return StopScanning, err
			}
			found = true
			fmt.Printf("updated page table root from %d to %d, triggered by %s\n", oldVal, pageId, tableName)
			return StopScanning, nil
		}
		return KeepScanning, nil
	})
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("unable to update page table entry for %d", pageId)
	}

	return nil
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

		newRootPg, err := bt.getRoot()
		if err != nil {
			return err
		}

		if newRootPg.pageID != schemaTablePg.pageID {
			if err := updatePageTable(fs, newRootPg.pageID, schemaTableName); err != nil {
				return err
			}
			schemaTablePg = newRootPg
		}
	}
	return nil
}

func NewFetcher() Fetcher {
	return fetch
}

func fetch(path string, tableName string) ([]*Row, []*Field, error) {

	fmt.Printf("Select query. Table: %s\n", tableName)

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

	fields := []*Field{}
	for _, fd := range rs.Fields {
		fields = append(fields, &Field{Column: fd.Name})
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

	bt.setRoot(pg)

	pgID := int32(0)
	found := false
	err = bt.scanRight(func(cell *keyValueCell) (bool, error) {
		tuple := Tuple{
			Relation: &pageTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return StopScanning, err
		}
		if tuple.Vals["table_name"] == relName {
			found = true
			pgID = tuple.Vals["page_id"].(int32)
			return StopScanning, nil
		}
		return KeepScanning, nil
	})

	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrTableNotExist
	}

	return pgID, nil
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

	bt.setRoot(pg)

	r := &Relation{}

	err = bt.scanRight(func(cell *keyValueCell) (bool, error) {
		tuple := Tuple{
			Relation: &schemaTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return StopScanning, err
		}
		if tuple.Vals["table_name"] == relName {
			r.Fields = append(r.Fields, FieldDef{
				Name:     tuple.Vals["field_name"].(string),
				Len:      tuple.Vals["field_length"].(int32),
				DataType: DataType(tuple.Vals["field_type"].(int32)),
			})
		}
		return KeepScanning, nil
	})
	if err != nil {
		return nil, err
	}

	return r, nil
}

type Field struct {
	TableID string
	Column  interface{}
}

func (f *Field) String() string {
	if f.TableID != "" {
		return fmt.Sprintf("%s.%s", f.TableID, f.Column)
	}
	return fmt.Sprintf("%s", f.Column)
}

type Fields []*Field

func (fields Fields) LookupFieldIdx(fieldName string) (int, error) {
	foundIdx := -1
	for idx, f := range fields {
		if f.Column == fieldName {
			if foundIdx > -1 {
				return -1, fmt.Errorf("%w: %s", ErrFieldAmbiguous, fieldName)
			}
			foundIdx = idx
		}
	}

	if foundIdx == -1 {
		return -1, fmt.Errorf("%w: %s", ErrFieldNotFound, fieldName)
	}

	return foundIdx, nil
}

func (fields Fields) LookupColIdxByID(tableID string, fieldName string) (int, error) {
	for idx, f := range fields {
		if f.Column == fieldName && f.TableID == tableID {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("%w: %s", ErrFieldNotFound, fieldName)
}

type Row struct {
	RowID uint32
	Vals  []interface{}
}

func (r *Row) String() string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, str := range r.Vals {
		sb.WriteString(fmt.Sprint(str))
		if i < len(r.Vals)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteByte(']')
	sb.WriteByte('\n')
	return sb.String()
}

func (r *Row) Merge(row *Row) *Row {
	newRow := &Row{}
	newRow.Vals = append(newRow.Vals, r.Vals...)
	newRow.Vals = append(newRow.Vals, row.Vals...)
	return newRow
}

func scanRelation(fs *fileStore, pageID uint32, r *Relation, fields Fields) ([]*Row, error) {
	bt := BTree{store: fs}

	// retrieve page table
	pg, err := fs.fetch(pageID)
	if err != nil {
		return nil, err
	}

	bt.setRoot(pg)

	var results []*Row

	err = bt.scanRight(func(cell *keyValueCell) (bool, error) {
		tuple := Tuple{
			Relation: r,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return StopScanning, err
		}
		row := &Row{
			RowID: cell.key,
		}
		for _, field := range fields {
			row.Vals = append(row.Vals, tuple.Vals[field.Column.(string)])
		}
		results = append(results, row)
		return KeepScanning, nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

func validateFields(r *Relation, fields Fields) error {
	allowed := make(map[string]bool, len(r.Fields))

	for _, fd := range r.Fields {
		allowed[fd.Name] = true
	}

	for _, field := range fields {
		if _, ok := allowed[field.Column.(string)]; !ok {
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

	if len(cols) == 0 {
		// fill in column list if omitted from INSERT query
		for _, fd := range schema.Fields {
			cols = append(cols, fd.Name)
		}
	}

	if len(cols) != len(vals) {
		return ErrColCountMismatch
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
	curPage, err := bt.getRoot()
	if err != nil {
		return err
	}

	if curPage.pageID != tablePg.pageID {
		if err := updatePageTable(fs, curPage.pageID, tableName); err != nil {
			return err
		}
	}

	return nil
}

// todo combine with update page table code?
func Update(path string, tableName string, rowID uint32, cols []string, updateSrc []interface{}) error {
	fs := &fileStore{path: path}
	if err := fs.open(); err != nil {
		return err
	}

	pgID, err := getRelationPageID(fs, tableName)
	if err != nil {
		return err
	}

	pg, err := fs.fetch(uint32(pgID))
	if err != nil {
		return err
	}

	r, err := getRelationSchema(fs, tableName)
	if err != nil {
		return err
	}

	bt := BTree{store: fs}
	bt.setRoot(pg)

	err = bt.scanRight(func(cell *keyValueCell) (bool, error) {
		if cell.key != rowID {
			return KeepScanning, nil
		}
		tuple := Tuple{
			Relation: r,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return StopScanning, err
		}

		for i, col := range cols {
			tuple.Vals[col] = updateSrc[i]
		}

		buf, err := tuple.Encode()
		if err != nil {
			return StopScanning, err
		}

		if err := cell.pg.updateCell(cell.key, buf.Bytes()); err != nil {
			return StopScanning, err
		}
		if err := fs.update(cell.pg); err != nil {
			return StopScanning, err
		}
		return KeepScanning, nil
	})

	if err != nil {
		return err
	}

	return nil
}
