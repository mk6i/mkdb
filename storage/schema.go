package btree

import (
	"bytes"
	"encoding/binary"
)

type DataType uint8

const (
	TYPE_INT = iota
	TYPE_VARCHAR
)

type FieldDef struct {
	dtype DataType
	name  string
	len   int32
}

type TableSchema struct {
	Fields []FieldDef
}

func (r *TableSchema) Encode() (*bytes.Buffer, error) {
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

func (r *TableSchema) Decode(buf *bytes.Buffer) error {
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
