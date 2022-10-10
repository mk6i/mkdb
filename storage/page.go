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

const (
	// maximum size (in bytes) of serialized page
	pageSize = 4096

	// size (in bytes) of fixed space used to store page metadata
	pageHeaderSize = 4 + // field: pageID
		1 + // field: cellType
		4 + // field: rightOffset
		1 + // field: hasLSib
		1 + // field: hasRSib
		4 + // field: lSibPageID
		4 + // field: rSibPageID
		4 + // field: cellCount
		2 // field: freeSize

	// size (in bytes) of offset array element
	offsetElemSize = 2

	// size (in bytes) of key cell
	keyCellSize = 4 + // field: key
		4 // field: pageID

	// size (in bytes) of key-value cell
	keyValueSize = 4 + // field: key
		4 + // field: alueSize
		400 // field: value (maximum size in bytes)

	// maximum number of non-leaf node elements
	maxInternalNodeCells = (pageSize - pageHeaderSize) / (offsetElemSize + keyCellSize)

	// maximum number of leaf node elements
	maxLeafNodeCells = (pageSize - pageHeaderSize) / (offsetElemSize + keyValueSize)
)

type keyCell struct {
	key    uint32
	pageID uint32
}

type keyValueCell struct {
	key        uint32
	valueSize  uint32
	valueBytes []byte
	pg         *page
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

func (p *page) updateCell(key uint32, value []byte) error {
	offset, found := p.findCellOffsetByKey(key)
	if !found {
		return fmt.Errorf("unable to find record to update for key %d", key)
	}
	p.cells[offset].(*keyValueCell).valueBytes = value
	p.cells[offset].(*keyValueCell).valueSize = uint32(len(value))
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

func (p *page) isFull() bool {
	switch p.cellType {
	case KeyCell:
		return len(p.offsets) >= maxInternalNodeCells
	case KeyValueCell:
		return len(p.offsets) >= maxLeafNodeCells
	default:
		panic("unexpected cell type")
	}
}

func (p *page) split(newPg *page) (uint32, error) {

	if p.cellType == KeyValueCell {
		mid := len(p.offsets) / 2

		for i := mid; i < len(p.offsets); i++ {
			cell := p.cells[p.offsets[i]].(*keyValueCell)
			if err := newPg.appendCell(cell.key, cell.valueBytes); err != nil {
				return 0, err
			}
		}

		p.offsets = p.offsets[0:mid]
		// todo make old cells reusable
		cell := newPg.cells[newPg.offsets[0]].(*keyValueCell)
		return cell.key, nil
	}

	// else keycell...
	mid := len(p.offsets) / 2

	for i := mid + 1; i < len(p.offsets); i++ {
		cell := p.cells[p.offsets[i]].(*keyCell)
		if err := newPg.appendKeyCell(cell.key, cell.pageID); err != nil {
			return 0, err
		}
	}

	newPg.setRightMostKey(p.rightOffset)
	key := p.cells[mid].(*keyCell).key
	p.setRightMostKey(p.cells[mid].(*keyCell).pageID)

	p.offsets = p.offsets[0:mid]
	// todo make old cells reusable

	return key, nil
}

func (p *page) getRightmostKey() uint32 {
	return p.cells[p.offsets[len(p.offsets)-1]].(*keyCell).key
}

func (p *page) encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, p.pageID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.cellType); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.rightOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.hasLSib); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.hasRSib); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.lSibPageID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.rSibPageID); err != nil {
		return nil, err
	}

	cellCount := uint32(len(p.offsets))
	if err := binary.Write(buf, binary.LittleEndian, cellCount); err != nil {
		return nil, err
	}
	for i := 0; i < len(p.offsets); i++ {
		if err := binary.Write(buf, binary.LittleEndian, p.offsets[i]); err != nil {
			return nil, err
		}
	}

	bufFooter := &bytes.Buffer{}

	for i := uint32(0); i < cellCount; i++ {
		switch p.cellType {
		case KeyCell:
			keyCell := p.cells[p.offsets[i]].(*keyCell)
			if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.key); err != nil {
				return nil, err
			}
			if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.pageID); err != nil {
				return nil, err
			}
		case KeyValueCell:
			keyCell := p.cells[p.offsets[i]].(*keyValueCell)
			if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.key); err != nil {
				return nil, err
			}
			if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.valueSize); err != nil {
				return nil, err
			}
			if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.valueBytes); err != nil {
				return nil, err
			}
		default:
			panic("unexpected cell type")
		}
	}

	freeSize := uint16(pageSize - buf.Len() - bufFooter.Len() - 2)

	// write out the free buffer, which separates the header
	if err := binary.Write(buf, binary.LittleEndian, freeSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, make([]byte, freeSize)); err != nil {
		return nil, err
	}

	if _, err := buf.Write(bufFooter.Bytes()); err != nil {
		return nil, err
	}

	if buf.Len() != pageSize {
		panic(fmt.Sprintf("page size is not %d bytes, got %d\n", pageSize, buf.Len()))
	}

	return buf, nil
}

func (p *page) decode(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.LittleEndian, &p.pageID); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.cellType); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.rightOffset); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.hasLSib); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.hasRSib); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.lSibPageID); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.rSibPageID); err != nil {
		return err
	}

	var cellCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &cellCount); err != nil {
		return err
	}
	for i := uint32(0); i < cellCount; i++ {
		var offset uint16
		if err := binary.Read(buf, binary.LittleEndian, &offset); err != nil {
			return err
		}
		p.offsets = append(p.offsets, offset)
	}

	if err := binary.Read(buf, binary.LittleEndian, &p.freeSize); err != nil {
		return err
	}

	buf.Next(int(p.freeSize))

	p.cells = make([]interface{}, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		switch p.cellType {
		case KeyCell:
			cell := &keyCell{}
			if err := binary.Read(buf, binary.LittleEndian, &cell.key); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.LittleEndian, &cell.pageID); err != nil {
				return err
			}
			p.cells[p.offsets[i]] = cell
		case KeyValueCell:
			cell := &keyValueCell{}
			if err := binary.Read(buf, binary.LittleEndian, &cell.key); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.LittleEndian, &cell.valueSize); err != nil {
				return err
			}
			strBuf := make([]byte, cell.valueSize)
			if _, err := buf.Read(strBuf); err != nil {
				return err
			}
			cell.valueBytes = strBuf
			p.cells[p.offsets[i]] = cell
		default:
			panic("unexpected cell type")
		}
	}

	return nil
}

type store interface {
	append(p *page) error
	update(p *page) error
	fetch(offset uint32) (*page, error)
	getLastKey() uint32
	incrementLastKey() error
	getRoot() (*page, error)
	setRoot(pg *page)
	setPageTableRoot(pg *page) error
}

type memoryStore struct {
	pages   []*page
	lastKey uint32
	root    *page
}

func (m *memoryStore) getRoot() (*page, error) {
	return m.root, nil
}

func (m *memoryStore) setRoot(pg *page) {
	m.root = pg
}

func (m *memoryStore) getLastKey() uint32 {
	return m.lastKey
}

func (m *memoryStore) incrementLastKey() error {
	m.lastKey++
	return nil
}

func (m *memoryStore) setPageTableRoot(pg *page) error {
	return nil
}

func (m *memoryStore) append(p *page) error {
	p.pageID = uint32(len(m.pages))
	m.pages = append(m.pages, p)
	return nil
}

func (m *memoryStore) update(p *page) error {
	return nil
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
	nextFreeOffset uint32
	pageTableRoot  uint32
}

func (f *fileStore) getRoot() (*page, error) {
	pg, err := f.fetch(f.rootOffset)
	return pg, err
}

func (f *fileStore) setRoot(pg *page) {
	f.rootOffset = pg.pageID
}

func (f *fileStore) setPageTableRoot(pg *page) error {
	f.pageTableRoot = pg.pageID
	return f.save()
}

func (f *fileStore) getLastKey() uint32 {
	return f.lastKey
}

func (f *fileStore) incrementLastKey() error {
	f.lastKey++
	return f.save()
}

func (f *fileStore) update(p *page) error {
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(p.pageID), 0)
	if err != nil {
		return err
	}

	buf, err := p.encode()
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())

	return err
}

func (f *fileStore) append(p *page) error {

	p.pageID = f.nextFreeOffset

	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(f.nextFreeOffset), 0)
	if err != nil {
		return err
	}

	buf, err := p.encode()
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	f.nextFreeOffset += pageSize

	return nil
}

func (f *fileStore) fetch(offset uint32) (*page, error) {
	file, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, pageSize)
	_, err = file.ReadAt(buf, int64(offset))
	if err != nil && err != io.EOF {
		return nil, err
	}

	pg := &page{}
	if err := pg.decode(bytes.NewBuffer(buf)); err != nil {
		return nil, err
	}

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
	err = binary.Write(writer, binary.LittleEndian, f.pageTableRoot)
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
	err = binary.Read(file, binary.LittleEndian, &f.pageTableRoot)
	if err != nil {
		return err
	}
	err = binary.Read(file, binary.LittleEndian, &f.nextFreeOffset)
	if err != nil {
		return err
	}

	return nil
}
