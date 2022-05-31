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
	pageID      uint32
	cellCount   uint32
	offsets     []uint16
	freeSize    uint16
	cells       []interface{}
	rightOffset uint32
}

func (p *page) setRightMostKey(pageID uint32) {
	p.rightOffset = pageID
}

func (p *page) cellKey(offset uint16) uint32 {
	switch {
	case p.cellType == KeyCell:
		return p.cells[offset].(*keyCell).key
	case p.cellType == KeyValueCell:
		return p.cells[offset].(*keyValueCell).key
	default:
		panic("unsupported keyValueCell binary search")
	}
}

func (p *page) appendKeyCell(key uint32, pageID uint32) error {
	offset := len(p.offsets)
	p.offsets = append(p.offsets, uint16(offset))
	p.cells = append(p.cells, &keyCell{
		key:    key,
		pageID: pageID,
	})
	return nil
}

func (p *page) appendCell(key uint32, value []byte) error {
	offset := len(p.offsets)
	p.offsets = append(p.offsets, uint16(offset))
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
	high := len(p.offsets) - 1

	for low <= high {
		mid := low + (high-low)/2
		midVal := p.cellKey(p.offsets[mid])
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

func (p *page) isFull(branchFactor uint32) bool {
	return len(p.offsets) >= int(branchFactor)
}

func (p *page) split(newPg *page) uint32 {

	mid := len(p.offsets) / 2

	for i := mid; i < len(p.offsets); i++ {
		if p.cellType == KeyCell {
			cell := p.cells[p.offsets[i]].(*keyCell)
			newPg.appendKeyCell(cell.key, cell.pageID)
		} else {
			cell := p.cells[p.offsets[i]].(*keyValueCell)
			newPg.appendCell(cell.key, cell.valueBytes)
		}
	}

	p.offsets = p.offsets[0:mid]
	p.cells = p.cells[0:mid]

	var key uint32

	if p.cellType == KeyCell {
		cell := newPg.cells[p.offsets[0]].(*keyCell)
		key = cell.key
	} else {
		cell := newPg.cells[p.offsets[0]].(*keyValueCell)
		key = cell.key
	}

	return key
}

type pageBuffer struct {
	buf *bytes.Buffer
}

func (r *pageBuffer) encode(p *page) error {

	binary.Write(r.buf, binary.LittleEndian, p.cellType)
	cellCount := uint32(len(p.offsets))
	binary.Write(r.buf, binary.LittleEndian, cellCount)
	for i := 0; i < len(p.offsets); i++ {
		binary.Write(r.buf, binary.LittleEndian, p.offsets[i])
	}

	freeSize := int16(4096 - r.buf.Len() - 2)
	freeSize -= int16(cellCount * 8)

	// write out the free buffer, which separates the header
	binary.Write(r.buf, binary.LittleEndian, freeSize)
	binary.Write(r.buf, binary.LittleEndian, make([]byte, freeSize))

	for i := uint32(0); i < cellCount; i++ {
		keyCell := p.cells[p.offsets[i]].(keyCell)
		binary.Write(r.buf, binary.LittleEndian, keyCell.key)
		binary.Write(r.buf, binary.LittleEndian, keyCell.pageID)
	}

	return nil
}

func (r *pageBuffer) decode() *page {

	p := &page{}

	binary.Read(r.buf, binary.LittleEndian, &p.cellType)
	var cellCount uint32
	binary.Read(r.buf, binary.LittleEndian, &cellCount)
	for i := uint32(0); i < cellCount; i++ {
		var offset uint16
		binary.Read(r.buf, binary.LittleEndian, &offset)
		p.offsets = append(p.offsets, offset)
	}

	binary.Read(r.buf, binary.LittleEndian, &p.freeSize)

	r.buf.Next(int(p.freeSize))

	p.cells = make([]interface{}, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		keyCell := keyCell{}
		binary.Read(r.buf, binary.LittleEndian, &keyCell.key)
		binary.Read(r.buf, binary.LittleEndian, &keyCell.pageID)
		p.cells[p.offsets[i]] = keyCell
	}

	return p
}

type store interface {
	append(p *page) (uint32, error)
	fetch(offset uint32) (*page, error)
	getLastKey() uint32
	incrementLastKey()
	getRoot() *page
	getBranchFactor() uint32
	setRoot(pg *page)
}

type memoryStore struct {
	pages        []*page
	lastKey      uint32
	root         *page
	branchFactor uint32
}

func (m *memoryStore) getBranchFactor() uint32 {
	return m.branchFactor
}

func (m *memoryStore) getRoot() *page {
	return m.root
}

func (m *memoryStore) setRoot(pg *page) {
	m.root = pg
}

func (m *memoryStore) getLastKey() uint32 {
	return m.lastKey
}

func (m *memoryStore) incrementLastKey() {
	m.lastKey++
}

func (m *memoryStore) append(p *page) (uint32, error) {
	pageID := uint32(len(m.pages))
	p.pageID = pageID
	m.pages = append(m.pages, p)
	return pageID, nil
}

func (m *memoryStore) fetch(offset uint32) (*page, error) {
	if int(offset) >= len(m.pages) {
		return nil, errors.New("page does not exist in store")
	}
	return m.pages[offset], nil
}
