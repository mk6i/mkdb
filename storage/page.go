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
	key        uint32
	valueSize  uint32
	valueBytes []byte
}

type page struct {
	cellType
	cellCount uint32
	offsets   []uint16
	freeSize  uint16
	cells     []interface{}
}

func (p *page) cellKey(offset int) uint32 {
	switch {
	case p.cellType == KeyCell:
		return p.cells[offset].(*keyCell).key
	case p.cellType == KeyValueCell:
		return p.cells[offset].(*keyValueCell).key
	default:
		panic("unsupported keyValueCell binary search")
	}
}

func (p *page) appendCell(key uint32, value []byte) error {
	p.offsets = append(p.offsets, uint16(key))
	p.cells = append(p.cells, &keyValueCell{
		key:        key,
		valueSize:  uint32(len(value)),
		valueBytes: value,
	})
	return nil
}

func (p *page) getCellValue(offset int) []byte {
	return p.cells[offset].(*keyValueCell).valueBytes
}

// findCellOffsetByKey searches for a cell by key. if found is true, offset is the
// position of key in the cell slice. if found is false, offset is key's
// insertion point (the index of the first element greater than key).
func (p *page) findCellOffsetByKey(key uint32) (offset int, found bool) {
	low := 0
	high := len(p.cells) - 1

	for low <= high {
		mid := low + (high-low)/2
		midVal := p.cellKey(mid)
		switch {
		case midVal == key:
			return mid, true
		case midVal < key:
			low = mid + 1
		default:
			high = mid - 1
		}
	}

	return low, false
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
	getLastKey() uint32
	incrementLastKey()
}

type memoryStore struct {
	pages   []*page
	lastKey uint32
}

func (m *memoryStore) getLastKey() uint32 {
	return m.lastKey
}

func (m *memoryStore) incrementLastKey() {
	m.lastKey++
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
