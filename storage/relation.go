package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
)

type DataType uint8

const (
	TypeInt = iota
	TypeVarchar
)

const (
	initialPageTableOffset   = pageSize
	initialSchemaTableOffset = initialPageTableOffset * 2
	pageTableName            = "sys_pages"
	schemaTableName          = "sys_schema"
)

var (
	ErrColCountMismatch  = errors.New("value list count does not match column list count")
	ErrDBExists          = errors.New("database already exists")
	ErrDBNotExist        = errors.New("database does not exist")
	ErrDBNotSelected     = errors.New("database not been selected")
	ErrFieldAmbiguous    = errors.New("field is ambiguous")
	ErrFieldNotFound     = errors.New("field not found")
	ErrTableAlreadyExist = errors.New("table already exists")
	ErrTableNotExist     = errors.New("table does not exist")
	ErrTypeMismatch      = errors.New("types do not match")
)

type FieldDef struct {
	DataType
	Name string
	Len  int32
}

func (f *FieldDef) Validate(val interface{}) error {
	switch f.DataType {
	case TypeInt:
		if reflect.TypeOf(val).Kind() != reflect.Int32 {
			return ErrTypeMismatch
		}
	case TypeVarchar:
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
			DataType: TypeVarchar,
			Len:      255,
		},
		{
			Name:     "file_offset",
			DataType: TypeInt,
		},
	},
}

var schemaTableSchema = Relation{
	Fields: []FieldDef{
		{
			Name:     "table_name",
			DataType: TypeVarchar,
			Len:      255,
		},
		{
			Name:     "field_name",
			DataType: TypeVarchar,
			Len:      255,
		},
		{
			Name:     "field_type",
			DataType: TypeInt,
		},
		{
			Name:     "field_length",
			DataType: TypeInt,
			Len:      255,
		},
	},
}

type Relation struct {
	Fields []FieldDef
}

func (r *Relation) Encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, uint16(len(r.Fields))); err != nil {
		return buf, err
	}

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
		if fd.DataType == TypeVarchar {
			if err := binary.Write(buf, binary.LittleEndian, fd.Len); err != nil {
				return buf, err
			}
		}
	}
	return buf, nil
}

func (r *Relation) Decode(buf *bytes.Buffer) error {
	var fieldCount uint16
	if err := binary.Read(buf, binary.LittleEndian, &fieldCount); err != nil {
		return err
	}

	for i := uint16(0); i < fieldCount; i++ {
		fd := FieldDef{}
		if err := binary.Read(buf, binary.LittleEndian, &fd.DataType); err != nil {
			return err
		}

		var nameLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
			return err
		}

		strBuf := make([]byte, nameLen)
		_, err := buf.Read(strBuf)
		if err != nil {
			return err
		}

		fd.Name = string(strBuf)

		if fd.DataType == TypeVarchar {
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
		case TypeInt:
			if err := binary.Write(buf, binary.LittleEndian, val); err != nil {
				return buf, err
			}
		case TypeVarchar:
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
		case TypeInt:
			var val int32
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				return err
			}
			v = val
		case TypeVarchar:
			var strLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return err
			}
			strBuf := make([]byte, strLen)
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

type RelationService struct {
	fs *fileStore
}

func OpenRelation(dbName string) (*RelationService, error) {
	path, exists, err := dbPath(dbName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrDBNotExist
	}
	fs := &fileStore{
		path: path,
	}
	if err := fs.open(); err != nil {
		return nil, err
	}
	return &RelationService{
		fs: fs,
	}, nil
}

func CreateDB(dbName string) error {
	if err := os.Mkdir("data", 0755); err != nil && !os.IsExist(err) {
		return fmt.Errorf("error making data dir: %s", err.Error())
	}

	path, exists, err := dbPath(dbName)
	if err != nil {
		return err
	}
	if exists {
		return ErrDBExists
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	defer file.Close()

	fs := &fileStore{
		path:           path,
		nextFreeOffset: pageSize,
	}

	if err := fs.save(); err != nil {
		return err
	}

	rs := &RelationService{fs: fs}

	// create page table page
	pgTblPg, err := rs.createPage()
	if err != nil {
		return err
	}
	if pgTblPg.getFileOffset() != initialPageTableOffset {
		return fmt.Errorf("expected page table to be first page at offset %d, at offset %d instead", initialPageTableOffset, pgTblPg.getFileOffset())
	}
	if err := rs.fs.setPageTableRoot(pgTblPg); err != nil {
		return err
	}
	if err := rs.insertPageTable(pgTblPg, pageTableName); err != nil {
		return err
	}

	// create schema table page
	schemaTblPg, err := rs.createPage()
	if err != nil {
		return err
	}
	if schemaTblPg.getFileOffset() != initialSchemaTableOffset {
		return fmt.Errorf("expected page table to be second page at offset %d, at offset %d instead", initialSchemaTableOffset, schemaTblPg.getFileOffset())
	}
	if err := rs.insertPageTable(schemaTblPg, schemaTableName); err != nil {
		return err
	}

	if err := rs.insertSchemaTable(&pageTableSchema, pageTableName); err != nil {
		return err
	}
	if err := rs.insertSchemaTable(&schemaTableSchema, schemaTableName); err != nil {
		return err
	}

	return nil
}

func (rs *RelationService) CreateTable(r *Relation, tableName string) error {
	_, err := rs.getRelationFileOffset(tableName)
	if err != ErrTableNotExist {
		return ErrTableAlreadyExist
	}

	pg, err := rs.createPage()
	if err != nil {
		return err
	}
	if err := rs.insertPageTable(pg, tableName); err != nil {
		return err
	}
	if err := rs.insertSchemaTable(r, tableName); err != nil {
		return err
	}
	return nil
}

func (rs *RelationService) createPage() (btreeNode, error) {
	rootPg := &leafNode{}

	if err := rs.fs.append(rootPg); err != nil {
		return rootPg, err
	}
	return rootPg, nil
}

func (rs *RelationService) insertPageTable(node btreeNode, tableName string) error {
	tuple := Tuple{
		Relation: &pageTableSchema,
		Vals: map[string]interface{}{
			"table_name":  tableName,
			"file_offset": int32(node.getFileOffset()), // todo wat?
		},
	}

	buf, err := tuple.Encode()
	if err != nil {
		return err
	}

	// insert page table record
	pgTablePg, err := rs.fs.fetch(rs.fs.pageTableRoot)
	if err != nil {
		return err
	}

	bt := &BTree{store: rs.fs}
	bt.setRoot(pgTablePg)

	id, err := bt.insert(buf.Bytes())
	if err != nil {
		return err
	}

	curRoot, err := bt.getRoot()
	if err != nil {
		return err
	}

	rootChanged := curRoot.getFileOffset() != pgTablePg.getFileOffset()
	if rootChanged {
		if err := rs.fs.setPageTableRoot(curRoot); err != nil {
			return err
		}
	}

	fmt.Printf("inserted new page table record for %s, page id: %d\n", tableName, id)

	return nil
}

func (rs *RelationService) updatePageTable(fileOffset uint64, tableName string) error {
	pgTablePg, err := rs.fs.fetch(rs.fs.pageTableRoot)
	if err != nil {
		return err
	}

	bt := &BTree{store: rs.fs}
	bt.setRoot(pgTablePg)

	found := false
	err = bt.scanRight(func(cell *leafNodeCell) (ScanAction, error) {
		tuple := Tuple{
			Relation: &pageTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return StopScanning, err
		}
		if tuple.Vals["table_name"] == tableName {
			oldVal := tuple.Vals["file_offset"]
			tuple.Vals["file_offset"] = int32(fileOffset)
			buf, err := tuple.Encode()
			if err != nil {
				return StopScanning, err
			}
			if err := cell.pg.(*leafNode).updateCell(cell.key, buf.Bytes()); err != nil {
				return StopScanning, err
			}
			if err := rs.fs.update(cell.pg); err != nil {
				return StopScanning, err
			}
			found = true
			fmt.Printf("updated page table root from %d to %d, triggered by %s\n", oldVal, fileOffset, tableName)
			return StopScanning, nil
		}
		return KeepScanning, nil
	})
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("unable to update page table entry for %d", fileOffset)
	}

	return nil
}

func (rs *RelationService) insertSchemaTable(r *Relation, tableName string) error {

	fileOffset, err := rs.getRelationFileOffset(schemaTableName)
	if err != nil {
		return err
	}

	schemaTablePg, err := rs.fs.fetch(uint64(fileOffset))
	if err != nil {
		return err
	}

	bt := &BTree{store: rs.fs}

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

		rootChanged := newRootPg.getFileOffset() != schemaTablePg.getFileOffset()
		if rootChanged {
			if err := rs.updatePageTable(newRootPg.getFileOffset(), schemaTableName); err != nil {
				return err
			}
			schemaTablePg = newRootPg
		}
	}
	return nil
}

func (rs *RelationService) Fetch(tableName string) ([]*Row, []*Field, error) {
	fmt.Printf("Select query. Table: %s\n", tableName)
	fmt.Printf("page table root offset: %d\n", rs.fs.pageTableRoot)

	fileOffset, err := rs.getRelationFileOffset(tableName)
	if err != nil {
		return nil, nil, err
	}

	fmt.Printf("relation %s page id: %d\n", tableName, fileOffset)

	schema, err := rs.getRelationSchema(tableName)
	if err != nil {
		return nil, nil, err
	}

	var fields []*Field
	for _, fd := range schema.Fields {
		fields = append(fields, &Field{Column: fd.Name})
	}

	fmt.Printf("relation %s schema: %v\n", tableName, schema)

	rows, err := rs.scanRelation(uint64(fileOffset), schema, fields)

	return rows, fields, err
}

func (rs *RelationService) getRelationFileOffset(relName string) (int32, error) {
	bt := BTree{store: rs.fs}

	// retrieve page table
	pg, err := rs.fs.fetch(rs.fs.pageTableRoot)
	if err != nil {
		return 0, err
	}

	bt.setRoot(pg)

	fileOffset := int32(0)
	found := false
	err = bt.scanRight(func(cell *leafNodeCell) (ScanAction, error) {
		tuple := Tuple{
			Relation: &pageTableSchema,
			Vals:     make(map[string]interface{}),
		}
		if err := tuple.Decode(bytes.NewBuffer(cell.valueBytes)); err != nil {
			return StopScanning, err
		}
		if tuple.Vals["table_name"] == relName {
			found = true
			fileOffset = tuple.Vals["file_offset"].(int32)
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

	return fileOffset, nil
}

func (rs *RelationService) getRelationSchema(relName string) (*Relation, error) {
	bt := BTree{store: rs.fs}

	schemaTblOffset, err := rs.getRelationFileOffset(schemaTableName)
	if err != nil {
		return nil, err
	}

	// retrieve page table
	pg, err := rs.fs.fetch(uint64(schemaTblOffset))
	if err != nil {
		return nil, err
	}

	bt.setRoot(pg)

	r := &Relation{}

	err = bt.scanRight(func(cell *leafNodeCell) (ScanAction, error) {
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

func (rs *RelationService) scanRelation(fileOffset uint64, r *Relation, fields Fields) ([]*Row, error) {
	bt := BTree{store: rs.fs}

	// retrieve page table
	pg, err := rs.fs.fetch(fileOffset)
	if err != nil {
		return nil, err
	}

	bt.setRoot(pg)

	var results []*Row

	err = bt.scanRight(func(cell *leafNodeCell) (ScanAction, error) {
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

func (rs *RelationService) Insert(tableName string, cols []string, vals []interface{}) error {
	fileOffset, err := rs.getRelationFileOffset(tableName)
	if err != nil {
		return err
	}

	tablePg, err := rs.fs.fetch(uint64(fileOffset))
	if err != nil {
		return err
	}

	schema, err := rs.getRelationSchema(tableName)
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

	bt := &BTree{store: rs.fs}
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

	rootChanged := curPage.getFileOffset() != tablePg.getFileOffset()
	if rootChanged {
		if err := rs.updatePageTable(curPage.getFileOffset(), tableName); err != nil {
			return err
		}
	}

	return nil
}

// todo combine with update page table code?
func (rs *RelationService) Update(tableName string, rowID uint32, cols []string, updateSrc []interface{}) error {

	fileOffset, err := rs.getRelationFileOffset(tableName)
	if err != nil {
		return err
	}

	pg, err := rs.fs.fetch(uint64(fileOffset))
	if err != nil {
		return err
	}

	r, err := rs.getRelationSchema(tableName)
	if err != nil {
		return err
	}

	bt := BTree{store: rs.fs}
	bt.setRoot(pg)

	err = bt.scanRight(func(cell *leafNodeCell) (ScanAction, error) {
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

		if err := cell.pg.(*leafNode).updateCell(cell.key, buf.Bytes()); err != nil {
			return StopScanning, err
		}
		if err := rs.fs.update(cell.pg); err != nil {
			return StopScanning, err
		}
		return KeepScanning, nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (rs *RelationService) MarkDeleted(tableName string, rowID uint32) error {
	fileOffset, err := rs.getRelationFileOffset(tableName)
	if err != nil {
		return err
	}

	pg, err := rs.fs.fetch(uint64(fileOffset))
	if err != nil {
		return err
	}

	bt := BTree{store: rs.fs}
	bt.setRoot(pg)

	cell, err := bt.findCell(rowID)
	if err != nil {
		return err
	}
	if cell == nil {
		return fmt.Errorf("unable to find cell for rowID %d", rowID)
	}

	cell.deleted = true

	if err := bt.update(cell.pg); err != nil {
		return err
	}

	return nil
}

func dbPath(db string) (string, bool, error) {
	if db == "" {
		return "", false, ErrDBNotSelected
	}

	path := "data/" + strings.ToLower(db)

	_, err := os.Stat(path)

	if err != nil && !os.IsNotExist(err) {
		return path, false, err
	}

	return path, !os.IsNotExist(err), nil
}
