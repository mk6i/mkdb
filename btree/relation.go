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
	pageTableOffset   = 4096
	schemaTableOffset = pageTableOffset * 2
	pageTableName     = "sys_pages"
	schemaTableName   = "sys_schema"
)

var (
	errTypeMismatch = errors.New("types do not match")
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
			return errTypeMismatch
		}
	case TYPE_VARCHAR:
		if reflect.TypeOf(val).Kind() != reflect.String {
			return errTypeMismatch
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
	if pgTblPg.pageID != pageTableOffset {
		return fmt.Errorf("expected page table to be first page at offset %d, at offset %d instead", pageTableOffset, pgTblPg.pageID)
	}

	// create schema table page
	schemaTblPg, err := createPage(fs)
	if err != nil {
		return err
	}
	if schemaTblPg.pageID != schemaTableOffset {
		return fmt.Errorf("expected page table to be first page at offset %d, at offset %d instead", schemaTableOffset, schemaTblPg.pageID)
	}

	// update page table and schema records
	if err := insertTableMetadata(fs, &pageTableSchema, pgTblPg.pageID, pageTableName); err != nil {
		return err
	}

	if err := insertTableMetadata(fs, &schemaTableSchema, schemaTblPg.pageID, schemaTableName); err != nil {
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
	if err := insertTableMetadata(fs, r, pg.pageID, tableName); err != nil {
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

func insertTableMetadata(fs *fileStore, r *Relation, pageId uint32, tableName string) error {

	// insert page table record
	pgTablePg, err := fs.fetch(pageTableOffset)
	if err != nil {
		return err
	}

	fs.setRoot(pgTablePg)

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

	bt := &BTree{store: fs}
	bt.setRoot(pgTablePg)
	id, err := bt.insert(buf.Bytes())
	if err != nil {
		return err
	}

	fmt.Printf("inserted new record into %s, id: %d\n", tableName, id)

	// insert schema table record
	schemaTablePg, err := fs.fetch(schemaTableOffset)
	if err != nil {
		return err
	}

	bt.setRoot(schemaTablePg)

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

		id, err := bt.insert(buf.Bytes())
		if err != nil {
			return err
		}
		fmt.Printf("inserted new record into %s, id: %d\n", tableName, id)
	}

	return nil
}
