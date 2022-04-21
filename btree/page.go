package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type cellType uint8

const (
	KeyCell = iota
	KeyValueCell
)

type keyCell struct {
	key    uint32
	pageID uint32
}

type keyValueCell struct {
	keySize    uint16
	valueSize  uint32
	keyBytes   []byte
	valueBytes []byte
}

type page struct {
	cellType
	cellCount uint32
	offsets   []uint16
	freeSize  uint16
	cells     []interface{}
}

type pageBuffer struct {
	buf *bytes.Buffer
}

func (r *pageBuffer) encode(p *page) error {

	binary.Write(r.buf, binary.LittleEndian, p.cellType)
	binary.Write(r.buf, binary.LittleEndian, p.cellCount)
	for i := uint32(0); i < p.cellCount; i++ {
		binary.Write(r.buf, binary.LittleEndian, p.offsets[i])
	}

	freeSize := int16(4096 - r.buf.Len() - 2)
	freeSize -= int16(p.cellCount * 8)

	// write out the free buffer, which separates the header
	binary.Write(r.buf, binary.LittleEndian, freeSize)
	binary.Write(r.buf, binary.LittleEndian, make([]byte, freeSize))

	for i := 0; i < int(p.cellCount); i++ {
		keyCell := p.cells[i].(keyCell)
		binary.Write(r.buf, binary.LittleEndian, keyCell.key)
		binary.Write(r.buf, binary.LittleEndian, keyCell.pageID)
	}

	return nil
}

func (r *pageBuffer) decode() *page {

	p := &page{}

	binary.Read(r.buf, binary.LittleEndian, &p.cellType)
	binary.Read(r.buf, binary.LittleEndian, &p.cellCount)
	for i := uint32(0); i < p.cellCount; i++ {
		var offset uint16
		binary.Read(r.buf, binary.LittleEndian, &offset)
		p.offsets = append(p.offsets, offset)
	}

	binary.Read(r.buf, binary.LittleEndian, &p.freeSize)

	r.buf.Next(int(p.freeSize))

	for i := 0; i < int(p.cellCount); i++ {
		keyCell := keyCell{}
		binary.Read(r.buf, binary.LittleEndian, &keyCell.key)
		binary.Read(r.buf, binary.LittleEndian, &keyCell.pageID)
		p.cells = append(p.cells, keyCell)
	}

	return p
}

type store interface {
	append(p *page) (uint32, error)
	fetch(offset uint16) (*page, error)
}

type memoryStore struct {
	pages []*page
}

func (m *memoryStore) append(p *page) (uint32, error) {
	m.pages = append(m.pages, p)
	return uint32(len(m.pages) - 1), nil
}

func (m *memoryStore) fetch(offset uint16) (*page, error) {
	if int(offset) >= len(m.pages) {
		return nil, errors.New("page does not exist in store")
	}
	return m.pages[offset], nil
}
