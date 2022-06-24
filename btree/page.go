package btree

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
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
	hasLSib     bool
	hasRSib     bool
	lSibPageID  uint32
	rSibPageID  uint32
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

func (p *page) insertKeyCell(offset uint32, key uint32, pageID uint32) error {
	p.offsets = append(p.offsets[:offset+1], p.offsets[offset:]...)
	p.offsets[offset] = uint16(len(p.cells))
	p.cells = append(p.cells, &keyCell{
		key:    key,
		pageID: pageID,
	})
	// todo: there has to be a better way to express this
	p.cells[p.offsets[offset]].(*keyCell).pageID, p.cells[p.offsets[offset+1]].(*keyCell).pageID = p.cells[p.offsets[offset+1]].(*keyCell).pageID, p.cells[p.offsets[offset]].(*keyCell).pageID

	return nil
}

func (p *page) insertCell(offset uint32, key uint32, value []byte) error {
	if uint32(len(p.offsets)) == offset { // nil or empty slice or after last element
		p.offsets = append(p.offsets, uint16(len(p.cells)))
	} else {
		p.offsets = append(p.offsets[:offset+1], p.offsets[offset:]...) // index < len(a)
		p.offsets[offset] = uint16(len(p.cells))
	}
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

	if p.cellType == KeyValueCell {
		mid := len(p.offsets) / 2

		for i := mid; i < len(p.offsets); i++ {
			cell := p.cells[p.offsets[i]].(*keyValueCell)
			newPg.appendCell(cell.key, cell.valueBytes)
		}

		p.offsets = p.offsets[0:mid]
		// todo make old cells reusable
		cell := newPg.cells[newPg.offsets[0]].(*keyValueCell)
		return cell.key
	}

	// else keycell...
	mid := len(p.offsets) / 2

	for i := mid + 1; i < len(p.offsets); i++ {
		cell := p.cells[p.offsets[i]].(*keyCell)
		newPg.appendKeyCell(cell.key, cell.pageID)
	}

	newPg.setRightMostKey(p.rightOffset)
	key := p.cells[mid].(*keyCell).key
	p.setRightMostKey(p.cells[mid].(*keyCell).pageID)

	p.offsets = p.offsets[0:mid]
	// todo make old cells reusable

	return key
}

func (p *page) getRightmostKey() uint32 {
	return p.cells[p.offsets[len(p.offsets)-1]].(*keyCell).key
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

type fileStore struct {
	path           string
	lastKey        uint32
	rootOffset     uint32
	branchFactor   uint32
	nextFreeOffset uint32
}

func (f *fileStore) getBranchFactor() uint32 {
	return f.branchFactor
}

func (f *fileStore) getRoot() *page {
	pg, err := f.fetch(f.rootOffset)
	if err != nil {
		panic(fmt.Sprintf("error fetching root: %s", err.Error()))
	}
	return pg
}

func (f *fileStore) setRoot(pg *page) {
	f.rootOffset = pg.pageID
	f.save()
}

func (f *fileStore) getLastKey() uint32 {
	return f.lastKey
}

func (f *fileStore) incrementLastKey() {
	f.lastKey++
	f.save()
}

func (f *fileStore) append(p *page) (uint32, error) {

	p.pageID = f.nextFreeOffset

	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	_, err = file.Seek(int64(f.nextFreeOffset), 0)
	if err != nil {
		return 0, err
	}

	buf := bytes.NewBuffer(make([]byte, 0))
	pb := &pageBuffer{
		buf: buf,
	}

	err = pb.encode(p)
	if err != nil {
		return 0, err
	}

	_, err = file.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}

	f.nextFreeOffset += 4096

	return p.pageID, nil
}

func (f *fileStore) fetch(offset uint32) (*page, error) {
	file, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, 4096)
	_, err = file.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		return nil, err
	}

	pb := &pageBuffer{
		buf: bytes.NewBuffer(buf),
	}

	pg := pb.decode()

	return pg, nil
}

func (f *fileStore) save() error {

	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(file)

	err = binary.Write(writer, binary.LittleEndian, f.lastKey)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.LittleEndian, f.rootOffset)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.LittleEndian, f.branchFactor)
	if err != nil {
		return err
	}
	err = binary.Write(writer, binary.LittleEndian, f.nextFreeOffset)
	if err != nil {
		return err
	}

	return writer.Flush()
}

func (f *fileStore) open() error {

	file, err := os.Open(f.path)
	if err != nil {
		return err
	}
	defer file.Close()

	err = binary.Read(file, binary.LittleEndian, &f.lastKey)
	if err != nil {
		return err
	}
	err = binary.Read(file, binary.LittleEndian, &f.rootOffset)
	if err != nil {
		return err
	}
	err = binary.Read(file, binary.LittleEndian, &f.branchFactor)
	if err != nil {
		return err
	}
	err = binary.Read(file, binary.LittleEndian, &f.nextFreeOffset)
	if err != nil {
		return err
	}

	return nil
}
