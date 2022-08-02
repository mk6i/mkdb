package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
)

type DataType uint8

const (
	TYPE_INT = iota
	TYPE_VARCHAR
)

var (
	errTypeMismatch = errors.New("types do not match")
)

type FieldDef struct {
	dtype DataType
	name  string
	len   int32
}

func (f *FieldDef) Validate(val interface{}) error {
	switch f.dtype {
	case TYPE_INT:
		if reflect.TypeOf(val).Kind() != reflect.Int32 {
			return errTypeMismatch
		}
	case TYPE_VARCHAR:
		if reflect.TypeOf(val).Kind() != reflect.String {
			return errTypeMismatch
		}
	}
	return nil
}

type Relation struct {
	Fields []FieldDef
}

func (r *Relation) Encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	binary.Write(buf, binary.LittleEndian, uint16(len(r.Fields)))

	for _, fd := range r.Fields {
		if err := binary.Write(buf, binary.LittleEndian, uint8(fd.dtype)); err != nil {
			return buf, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(fd.name))); err != nil {
			return buf, err
		}
		if err := binary.Write(buf, binary.LittleEndian, []byte(fd.name)); err != nil {
			return buf, err
		}
		if fd.dtype == TYPE_VARCHAR {
			if err := binary.Write(buf, binary.LittleEndian, fd.len); err != nil {
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
		if err := binary.Read(buf, binary.LittleEndian, &fd.dtype); err != nil {
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

		fd.name = string(strBuf)

		if fd.dtype == TYPE_VARCHAR {
			if err := binary.Read(buf, binary.LittleEndian, &fd.len); err != nil {
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
		val := r.Vals[fd.name]

		if err := fd.Validate(val); err != nil {
			return buf, err
		}

		switch fd.dtype {
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

		switch fd.dtype {
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
		r.Vals[fd.name] = v
	}

	return nil
}
