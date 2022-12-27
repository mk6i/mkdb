package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	InternalNode byte = iota
	LeafNode
)

const (
	// maximum size (in bytes) of serialized page
	pageSize = 4096

	// size (in bytes) of fixed space used to store page metadata
	internalNodeHeaderSize = 1 + // field: cellType
		8 + // field: fileOffset
		8 + // field: rightOffset
		4 + // field: cellCount
		2 // field: freeSize

	// size (in bytes) of fixed space used to store page metadata
	leafNodeHeaderSize = 1 + // field: cellType
		8 + // field: fileOffset
		1 + // field: hasLSib
		1 + // field: hasRSib
		8 + // field: lSibFileOffset
		8 + // field: rSibFileOffset
		4 + // field: cellCount
		2 // field: freeSize

	// size (in bytes) of offset array element
	offsetElemSize = 2

	// size (in bytes) of key cell
	internalNodeCellSize = 4 + // field: key
		8 // field: fileOffset

	// maximum size (in bytes) of key-value cell value
	maxValueSize = 400

	// size (in bytes) of key-value cell
	leafNodeCellSize = 4 + // field: key
		1 + // field: deleted
		4 + // field: valueSize
		maxValueSize

	// maximum number of non-leaf node elements
	maxInternalNodeCells = (pageSize - internalNodeHeaderSize) / (offsetElemSize + internalNodeCellSize)

	// maximum number of leaf node elements
	maxLeafNodeCells = (pageSize - leafNodeHeaderSize) / (offsetElemSize + leafNodeCellSize)
)

var ErrRowTooLarge = fmt.Errorf("row exceeds %d bytes", maxValueSize)

func checkRowSizeLimit(value []byte) error {
	if len(value) > maxValueSize {
		return ErrRowTooLarge
	}
	return nil
}

type internalNodeCell struct {
	key        uint32
	fileOffset uint64
}

type leafNodeCell struct {
	key        uint32
	valueSize  uint32
	valueBytes []byte
	pg         btreeNode
	deleted    bool
}

type node struct {
	fileOffset uint64
	offsets    []uint16
	freeSize   uint16
}

func (n *node) getFileOffset() uint64 {
	return n.fileOffset
}

func (n *node) setFileOffset(offset uint64) {
	n.fileOffset = offset
}

type btreeNode interface {
	getFileOffset() uint64
	setFileOffset(n uint64)
	encode() (*bytes.Buffer, error)
	decode(buf *bytes.Buffer) error
}

type internalNode struct {
	node
	cells       []*internalNodeCell
	rightOffset uint64
}

func (n *internalNode) setRightMostKey(fileOffset uint64) {
	n.rightOffset = fileOffset
}

func (n *internalNode) cellKey(offset uint16) uint32 {
	return n.cells[offset].key
}

func (n *internalNode) appendCell(key uint32, fileOffset uint64) error {
	offset := len(n.offsets)
	n.offsets = append(n.offsets, uint16(offset))
	n.cells = append(n.cells, &internalNodeCell{
		key:        key,
		fileOffset: fileOffset,
	})
	return nil
}

func (n *internalNode) insertCell(offset uint32, key uint32, fileOffset uint64) error {
	n.offsets = append(n.offsets[:offset+1], n.offsets[offset:]...)
	n.offsets[offset] = uint16(len(n.cells))
	n.cells = append(n.cells, &internalNodeCell{
		key:        key,
		fileOffset: fileOffset,
	})
	// todo: there has to be a better way to express this
	n.cells[n.offsets[offset]].fileOffset, n.cells[n.offsets[offset+1]].fileOffset = n.cells[n.offsets[offset+1]].fileOffset, n.cells[n.offsets[offset]].fileOffset
	return nil
}

// findCellOffsetByKey searches for a cell by key. if found is true, offset is the
// position of key in the cell slice. if found is false, offset is key's
// insertion point (the index of the first element greater than key).
func (n *internalNode) findCellOffsetByKey(key uint32) (offset int, found bool) {
	low := 0
	high := len(n.offsets) - 1

	for low <= high {
		mid := low + (high-low)/2
		midVal := n.cellKey(n.offsets[mid])
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

func (n *internalNode) isFull() bool {
	return len(n.offsets) >= maxInternalNodeCells
}

func (p *internalNode) split(newPg *internalNode) (uint32, error) {
	mid := len(p.offsets) / 2

	for i := mid + 1; i < len(p.offsets); i++ {
		cell := p.cells[p.offsets[i]]
		if err := newPg.appendCell(cell.key, cell.fileOffset); err != nil {
			return 0, err
		}
	}

	newPg.setRightMostKey(p.rightOffset)
	key := p.cells[mid].key
	p.setRightMostKey(p.cells[mid].fileOffset)

	p.offsets = p.offsets[0:mid]
	// todo make old cells reusable

	return key, nil
}

func (p *internalNode) getRightmostKey() uint32 {
	return p.cells[p.offsets[len(p.offsets)-1]].key
}

func (p *internalNode) encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, InternalNode); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.fileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.rightOffset); err != nil {
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
		keyCell := p.cells[p.offsets[i]]
		if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.key); err != nil {
			return nil, err
		}
		if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.fileOffset); err != nil {
			return nil, err
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

func (p *internalNode) decode(buf *bytes.Buffer) error {
	var cellType byte
	if err := binary.Read(buf, binary.LittleEndian, &cellType); err != nil {
		return err
	}
	if cellType != InternalNode {
		return fmt.Errorf("decoding error: expected node type %d, got %d", InternalNode, cellType)
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.fileOffset); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.rightOffset); err != nil {
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

	p.cells = make([]*internalNodeCell, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		cell := &internalNodeCell{}
		if err := binary.Read(buf, binary.LittleEndian, &cell.key); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &cell.fileOffset); err != nil {
			return err
		}
		p.cells[p.offsets[i]] = cell
	}

	return nil
}

type leafNode struct {
	node
	cells          []*leafNodeCell
	hasLSib        bool
	hasRSib        bool
	lSibFileOffset uint64
	rSibFileOffset uint64
}

func (n *leafNode) cellKey(offset uint16) uint32 {
	return n.cells[offset].key
}

func (n *leafNode) appendCell(key uint32, value []byte) error {
	offset := len(n.offsets)
	n.offsets = append(n.offsets, uint16(offset))
	n.cells = append(n.cells, &leafNodeCell{
		key:        key,
		valueSize:  uint32(len(value)),
		valueBytes: value,
	})
	return nil
}

func (n *leafNode) updateCell(key uint32, value []byte) error {
	if err := checkRowSizeLimit(value); err != nil {
		return err
	}
	offset, found := n.findCellOffsetByKey(key)
	if !found {
		return fmt.Errorf("unable to find record to update for key %d", key)
	}
	n.cells[offset].valueBytes = value
	n.cells[offset].valueSize = uint32(len(value))
	return nil
}

func (n *leafNode) insertCell(offset uint32, key uint32, value []byte) error {
	if err := checkRowSizeLimit(value); err != nil {
		return err
	}
	if uint32(len(n.offsets)) == offset { // nil or empty slice or after last element
		n.offsets = append(n.offsets, uint16(len(n.cells)))
	} else {
		n.offsets = append(n.offsets[:offset+1], n.offsets[offset:]...) // index < len(a)
		n.offsets[offset] = uint16(len(n.cells))
	}
	n.cells = append(n.cells, &leafNodeCell{
		key:        key,
		valueSize:  uint32(len(value)),
		valueBytes: value,
	})
	return nil
}

// findCellOffsetByKey searches for a cell by key. if found is true, offset is the
// position of key in the cell slice. if found is false, offset is key's
// insertion point (the index of the first element greater than key).
func (n *leafNode) findCellOffsetByKey(key uint32) (offset int, found bool) {
	low := 0
	high := len(n.offsets) - 1

	for low <= high {
		mid := low + (high-low)/2
		midVal := n.cellKey(n.offsets[mid])
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

func (n *leafNode) isFull() bool {
	return len(n.offsets) >= maxLeafNodeCells
}

func (p *leafNode) split(newPg *leafNode) (uint32, error) {
	mid := len(p.offsets) / 2

	for i := mid; i < len(p.offsets); i++ {
		cell := p.cells[p.offsets[i]]
		if err := newPg.appendCell(cell.key, cell.valueBytes); err != nil {
			return 0, err
		}
	}

	p.offsets = p.offsets[0:mid]
	// todo make old cells reusable
	cell := newPg.cells[newPg.offsets[0]]
	return cell.key, nil

}

func (p *leafNode) encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, LeafNode); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.fileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.hasLSib); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.hasRSib); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.lSibFileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.rSibFileOffset); err != nil {
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
		keyCell := p.cells[p.offsets[i]]
		if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.key); err != nil {
			return nil, err
		}
		if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.deleted); err != nil {
			return nil, err
		}
		if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.valueSize); err != nil {
			return nil, err
		}
		if err := binary.Write(bufFooter, binary.LittleEndian, keyCell.valueBytes); err != nil {
			return nil, err
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

func (p *leafNode) decode(buf *bytes.Buffer) error {
	var cellType byte
	if err := binary.Read(buf, binary.LittleEndian, &cellType); err != nil {
		return err
	}
	if cellType != LeafNode {
		return fmt.Errorf("decoding error: expected node type %d, got %d", LeafNode, cellType)
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.fileOffset); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.hasLSib); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.hasRSib); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.lSibFileOffset); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &p.rSibFileOffset); err != nil {
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

	p.cells = make([]*leafNodeCell, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		cell := &leafNodeCell{}
		if err := binary.Read(buf, binary.LittleEndian, &cell.key); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &cell.deleted); err != nil {
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
	}

	return nil
}

type store interface {
	append(p btreeNode) error
	update(p btreeNode) error
	fetch(offset uint64) (btreeNode, error)
	getLastKey() uint32
	incrementLastKey() error
	setPageTableRoot(pg btreeNode) error
}

type memoryStore struct {
	pages   []btreeNode
	lastKey uint32
}

func (m *memoryStore) getLastKey() uint32 {
	return m.lastKey
}

func (m *memoryStore) incrementLastKey() error {
	m.lastKey++
	return nil
}

func (m *memoryStore) setPageTableRoot(pg btreeNode) error {
	return nil
}

func (m *memoryStore) append(node btreeNode) error {
	node.setFileOffset(uint64(len(m.pages)))
	m.pages = append(m.pages, node)
	return nil
}

func (m *memoryStore) update(p btreeNode) error {
	return nil
}

func (m *memoryStore) fetch(offset uint64) (btreeNode, error) {
	if int(offset) >= len(m.pages) {
		return nil, errors.New("page does not exist in store")
	}
	return m.pages[offset], nil
}

func newFileStore(path string) *fileStore {
	return &fileStore{
		cache: NewLRU(1000),
		path:  path,
	}
}

type fileStore struct {
	path           string
	lastKey        uint32
	rootOffset     uint64
	nextFreeOffset uint64
	pageTableRoot  uint64
	cache          *LRUCache
}

func (f *fileStore) getRoot() (btreeNode, error) {
	return f.fetch(f.rootOffset)
}

func (f *fileStore) setRoot(node btreeNode) {
	f.rootOffset = node.getFileOffset()
}

func (f *fileStore) setPageTableRoot(node btreeNode) error {
	f.pageTableRoot = node.getFileOffset()
	return f.save()
}

func (f *fileStore) getLastKey() uint32 {
	return f.lastKey
}

func (f *fileStore) incrementLastKey() error {
	f.lastKey++
	return f.save()
}

func (f *fileStore) update(node btreeNode) error {
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(node.getFileOffset()), 0)
	if err != nil {
		return err
	}

	buf, err := node.encode()
	if err != nil {
		return err
	}
	_, err = file.Write(buf.Bytes())

	f.cache.set(node.getFileOffset(), node)

	return err
}

func (f *fileStore) append(node btreeNode) error {
	node.setFileOffset(f.nextFreeOffset)

	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(int64(f.nextFreeOffset), 0)
	if err != nil {
		return err
	}

	buf, err := node.encode()
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	f.cache.set(f.nextFreeOffset, node)

	f.nextFreeOffset += pageSize

	return nil
}

func (f *fileStore) fetch(offset uint64) (btreeNode, error) {
	if node, ok := f.cache.get(offset); ok {
		return node.(btreeNode), nil
	}

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

	var node btreeNode
	switch buf[0] {
	case InternalNode:
		node = &internalNode{}
	case LeafNode:
		node = &leafNode{}
	}

	if err := node.decode(bytes.NewBuffer(buf)); err != nil {
		return nil, err
	}

	return node, nil
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
