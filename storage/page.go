package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	InternalNode byte = iota
	LeafNode
)

const (
	// maximum size (in bytes) of serialized page
	pageSize = 4096

	// size (in bytes) of fixed space used to store internal node metadata
	internalNodeHeaderSize = 1 + // field: cellType
		8 + // field: fileOffset
		8 + // field: lastLSN
		8 + // field: rightOffset
		4 + // field: cellCount
		2 // field: freeSize

	// size (in bytes) of fixed space used to store leaf node metadata
	leafNodeHeaderSize = 1 + // field: cellType
		8 + // field: fileOffset
		8 + // field: lastLSN
		1 + // field: hasLSib
		1 + // field: hasRSib
		8 + // field: lSibFileOffset
		8 + // field: rSibFileOffset
		4 + // field: cellCount
		2 // field: freeSize

	// size (in bytes) of offset array element
	offsetElemSize = 2

	// size (in bytes) of key cell
	nodeCellSize = 4 + // field: key
		8 // field: fileOffset

	// maximum size (in bytes) of key-value cell value
	maxValueSize = 400

	// size (in bytes) of key-value cell
	leafNodeCellSize = 4 + // field: key
		1 + // field: deleted
		4 + // field: valueSize
		maxValueSize

	// maximum number of non-leaf node elements
	maxInternalNodeCells = (pageSize - internalNodeHeaderSize) / (offsetElemSize + nodeCellSize)

	// maximum number of leaf node elements
	maxLeafNodeCells = (pageSize - leafNodeHeaderSize) / (offsetElemSize + leafNodeCellSize)
)

// pageFlushInterval is how often to flush dirty pages to disk
const pageFlushInterval = 100 * time.Millisecond

var (
	ErrRowTooLarge  = fmt.Errorf("row exceeds %d bytes", maxValueSize)
	ErrLRUCacheFull = errors.New("cache is full and contains no evictable pages, try increasing page flush frequency")
)

func checkRowSizeLimit(value []byte) error {
	if len(value) > maxValueSize {
		return ErrRowTooLarge
	}
	return nil
}

type internalCell struct {
	key        uint32
	fileOffset uint64
}

type leafCell struct {
	key        uint32
	valueSize  uint32
	valueBytes []byte
	pg         *btreeNode
	deleted    bool
}

type btreeNode struct {
	fileOffset uint64
	offsets    []uint16
	freeSize   uint16
	dirty      bool
	lastLSN    uint64
	isLeaf     bool
	// internal node
	internalCells []*internalCell
	rightOffset   uint64
	// leaf node
	leafCells      []*leafCell
	hasLSib        bool
	hasRSib        bool
	lSibFileOffset uint64
	rSibFileOffset uint64
}

func (n *btreeNode) markDirty(lsn uint64) {
	n.lastLSN = lsn
	n.dirty = true
}

func (n *btreeNode) markClean() {
	n.dirty = false
}

func (n *btreeNode) isDirty() bool {
	return n.dirty
}

func (n *btreeNode) getFileOffset() uint64 {
	return n.fileOffset
}

func (n *btreeNode) setFileOffset(offset uint64) {
	n.fileOffset = offset
}

func (n *btreeNode) getLastLSN() uint64 {
	return n.lastLSN
}

func (n *btreeNode) setRightMostKey(fileOffset uint64) {
	n.rightOffset = fileOffset
}

func (n *btreeNode) cellKey(offset uint16) uint32 {
	if n.isLeaf {
		return n.leafCells[offset].key
	}
	return n.internalCells[offset].key
}

func (n *btreeNode) appendLeafCell(key uint32, value []byte) error {
	offset := len(n.offsets)
	n.offsets = append(n.offsets, uint16(offset))
	n.leafCells = append(n.leafCells, &leafCell{
		key:        key,
		valueSize:  uint32(len(value)),
		valueBytes: value,
	})
	return nil
}

func (n *btreeNode) appendInternalCell(key uint32, fileOffset uint64) error {
	offset := len(n.offsets)
	n.offsets = append(n.offsets, uint16(offset))
	n.internalCells = append(n.internalCells, &internalCell{
		key:        key,
		fileOffset: fileOffset,
	})
	return nil
}

func (n *btreeNode) insertInternalCell(offset uint32, key uint32, fileOffset uint64) error {
	n.offsets = append(n.offsets[:offset+1], n.offsets[offset:]...)
	n.offsets[offset] = uint16(len(n.internalCells))
	n.internalCells = append(n.internalCells, &internalCell{
		key:        key,
		fileOffset: fileOffset,
	})
	// todo: there has to be a better way to express this
	n.internalCells[n.offsets[offset]].fileOffset, n.internalCells[n.offsets[offset+1]].fileOffset = n.internalCells[n.offsets[offset+1]].fileOffset, n.internalCells[n.offsets[offset]].fileOffset
	return nil
}

func (n *btreeNode) insertLeafCell(offset uint32, key uint32, value []byte) error {
	if err := checkRowSizeLimit(value); err != nil {
		return err
	}
	if uint32(len(n.offsets)) == offset { // nil or empty slice or after last element
		n.offsets = append(n.offsets, uint16(len(n.leafCells)))
	} else {
		n.offsets = append(n.offsets[:offset+1], n.offsets[offset:]...) // index < len(a)
		n.offsets[offset] = uint16(len(n.leafCells))
	}
	n.leafCells = append(n.leafCells, &leafCell{
		key:        key,
		valueSize:  uint32(len(value)),
		valueBytes: value,
	})
	return nil
}

// findCellOffsetByKey searches for a cell by key. if found is true, offset is the
// position of key in the cell slice. if found is false, offset is key's
// insertion point (the index of the first element greater than key).
func (n *btreeNode) findCellOffsetByKey(key uint32) (offset int, found bool) {
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

func (n *btreeNode) isFull() bool {
	if n.isLeaf {
		return len(n.offsets) >= maxLeafNodeCells
	}
	return len(n.offsets) >= maxInternalNodeCells
}

func (n *btreeNode) getRightmostKey() uint32 {
	return n.internalCells[n.offsets[len(n.offsets)-1]].key
}

func (n *btreeNode) encode() (*bytes.Buffer, error) {
	if n.isLeaf {
		return n.encodeLeaf()
	}
	return n.encodeInternal()
}

func (n *btreeNode) encodeLeaf() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, LeafNode); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.fileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.lastLSN); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.hasLSib); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.hasRSib); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.lSibFileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.rSibFileOffset); err != nil {
		return nil, err
	}

	cellCount := uint32(len(n.offsets))
	if err := binary.Write(buf, binary.LittleEndian, cellCount); err != nil {
		return nil, err
	}
	for i := 0; i < len(n.offsets); i++ {
		if err := binary.Write(buf, binary.LittleEndian, n.offsets[i]); err != nil {
			return nil, err
		}
	}

	bufFooter := &bytes.Buffer{}

	for i := uint32(0); i < cellCount; i++ {
		keyCell := n.leafCells[n.offsets[i]]
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

func (n *btreeNode) encodeInternal() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, InternalNode); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.fileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.lastLSN); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, n.rightOffset); err != nil {
		return nil, err
	}

	cellCount := uint32(len(n.offsets))
	if err := binary.Write(buf, binary.LittleEndian, cellCount); err != nil {
		return nil, err
	}
	for i := 0; i < len(n.offsets); i++ {
		if err := binary.Write(buf, binary.LittleEndian, n.offsets[i]); err != nil {
			return nil, err
		}
	}

	bufFooter := &bytes.Buffer{}

	for i := uint32(0); i < cellCount; i++ {
		keyCell := n.internalCells[n.offsets[i]]
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

func (n *btreeNode) decode(buf *bytes.Buffer) error {
	if n.isLeaf {
		return n.decodeLeaf(buf)
	}
	return n.decodeInternal(buf)
}

func (n *btreeNode) decodeLeaf(buf *bytes.Buffer) error {
	var cellType byte
	if err := binary.Read(buf, binary.LittleEndian, &cellType); err != nil {
		return err
	}
	if cellType != LeafNode {
		return fmt.Errorf("decoding error: expected node type %d, got %d", LeafNode, cellType)
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.fileOffset); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.lastLSN); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.hasLSib); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.hasRSib); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.lSibFileOffset); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.rSibFileOffset); err != nil {
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
		n.offsets = append(n.offsets, offset)
	}

	if err := binary.Read(buf, binary.LittleEndian, &n.freeSize); err != nil {
		return err
	}

	buf.Next(int(n.freeSize))

	n.leafCells = make([]*leafCell, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		cell := &leafCell{}
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
		n.leafCells[n.offsets[i]] = cell
	}

	return nil
}

func (n *btreeNode) decodeInternal(buf *bytes.Buffer) error {
	var cellType byte
	if err := binary.Read(buf, binary.LittleEndian, &cellType); err != nil {
		return err
	}
	if cellType != InternalNode {
		return fmt.Errorf("decoding error: expected node type %d, got %d", InternalNode, cellType)
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.fileOffset); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.lastLSN); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &n.rightOffset); err != nil {
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
		n.offsets = append(n.offsets, offset)
	}

	if err := binary.Read(buf, binary.LittleEndian, &n.freeSize); err != nil {
		return err
	}

	buf.Next(int(n.freeSize))

	n.internalCells = make([]*internalCell, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		cell := &internalCell{}
		if err := binary.Read(buf, binary.LittleEndian, &cell.key); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.LittleEndian, &cell.fileOffset); err != nil {
			return err
		}
		n.internalCells[n.offsets[i]] = cell
	}

	return nil
}

func (n *btreeNode) updateCell(key uint32, value []byte) error {
	if err := checkRowSizeLimit(value); err != nil {
		return err
	}
	offset, found := n.findCellOffsetByKey(key)
	if !found {
		return fmt.Errorf("unable to find record to update for key %d", key)
	}
	n.leafCells[offset].valueBytes = value
	n.leafCells[offset].valueSize = uint32(len(value))
	return nil
}

func (n *btreeNode) split(newPg *btreeNode) (uint32, error) {
	if n.isLeaf {
		mid := len(n.offsets) / 2

		for i := mid; i < len(n.offsets); i++ {
			cell := n.leafCells[n.offsets[i]]
			if err := newPg.appendLeafCell(cell.key, cell.valueBytes); err != nil {
				return 0, err
			}
		}

		n.offsets = n.offsets[0:mid]
		// todo make old cells reusable
		cell := newPg.leafCells[newPg.offsets[0]]
		return cell.key, nil
	}

	mid := len(n.offsets) / 2

	for i := mid + 1; i < len(n.offsets); i++ {
		cell := n.internalCells[n.offsets[i]]
		if err := newPg.appendInternalCell(cell.key, cell.fileOffset); err != nil {
			return 0, err
		}
	}

	newPg.setRightMostKey(n.rightOffset)
	key := n.internalCells[mid].key
	n.setRightMostKey(n.internalCells[mid].fileOffset)

	n.offsets = n.offsets[0:mid]
	// todo make old cells reusable

	return key, nil
}

type store interface {
	append(p *btreeNode) error
	update(p *btreeNode) error
	fetch(offset uint64) (*btreeNode, error)
	getLastKey() uint32
	nextLSN() uint64
	incrLSN()
	incrementLastKey() error
	setPageTableRoot(pg *btreeNode) error
	flushPages() error
}

type memoryStore struct {
	pages   []*btreeNode
	lastKey uint32
}

func (m *memoryStore) getLastKey() uint32 {
	return m.lastKey
}

func (m *memoryStore) incrementLastKey() error {
	m.lastKey++
	return nil
}

func (m *memoryStore) setPageTableRoot(*btreeNode) error {
	return nil
}

func (m *memoryStore) append(node *btreeNode) error {
	node.setFileOffset(uint64(len(m.pages)))
	m.pages = append(m.pages, node)
	return nil
}

func (m *memoryStore) update(*btreeNode) error {
	return nil
}

func (m *memoryStore) fetch(offset uint64) (*btreeNode, error) {
	if int(offset) >= len(m.pages) {
		return nil, errors.New("page does not exist in store")
	}
	return m.pages[offset], nil
}

func (m *memoryStore) flushPages() error {
	return nil
}

func (m *memoryStore) nextLSN() uint64 {
	return 0
}

func (m *memoryStore) incrLSN() {

}

func newFileStore(path string, autoFlushCache bool) (*fileStore, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fs := &fileStore{
		autoFlushCache: autoFlushCache,
		cache:          NewLRU(10000),
		file:           file,
		mtx:            sync.RWMutex{},
	}
	if autoFlushCache {
		fs.tickerDone = make(chan bool)
		fs.ticker = time.NewTicker(pageFlushInterval)
		go func() {
			for {
				select {
				case <-fs.tickerDone:
					return
				case <-fs.ticker.C:
					if err := fs.flushPages(); err != nil {
						fmt.Printf("error flushing pages: %s", err.Error())
					}
				}
			}
		}()
	}
	return fs, nil
}

type fileStore struct {
	_nextLSN       uint64
	autoFlushCache bool
	cache          *LRUCache
	file           *os.File
	lastKey        uint32
	mtx            sync.RWMutex
	nextFreeOffset uint64
	pageTableRoot  uint64
	rootOffset     uint64
	ticker         *time.Ticker
	tickerDone     chan bool
}

func (f *fileStore) lockShared() {
	f.mtx.RLock()
}
func (f *fileStore) unlockShared() {
	f.mtx.RUnlock()
}

func (f *fileStore) lockExclusive() {
	f.mtx.Lock()
}
func (f *fileStore) unlockExclusive() {
	f.mtx.Unlock()
}

func (f *fileStore) close() error {
	defer f.file.Close()
	if f.autoFlushCache {
		f.ticker.Stop()
		f.tickerDone <- true
	}
	return f.flushPages()
}

func (f *fileStore) getRoot() (*btreeNode, error) {
	return f.fetch(f.rootOffset)
}

func (f *fileStore) setRoot(node *btreeNode) {
	f.rootOffset = node.getFileOffset()
}

func (f *fileStore) setPageTableRoot(node *btreeNode) error {
	f.pageTableRoot = node.getFileOffset()
	return nil
}

func (f *fileStore) getLastKey() uint32 {
	return f.lastKey
}

func (f *fileStore) incrementLastKey() error {
	f.lastKey++
	return nil
}

func (f *fileStore) update(node *btreeNode) error {
	buf, err := node.encode()
	if err != nil {
		return err
	}
	if _, err := f.file.WriteAt(buf.Bytes(), int64(node.getFileOffset())); err != nil {
		return err
	}

	if err := f.setCache(node.getFileOffset(), node); err != nil {
		return err
	}

	return err
}

func (f *fileStore) append(node *btreeNode) error {
	node.setFileOffset(f.nextFreeOffset)

	if err := f.setCache(node.getFileOffset(), node); err != nil {
		return err
	}

	f.nextFreeOffset += pageSize

	return nil
}

func (f *fileStore) fetch(offset uint64) (*btreeNode, error) {
	if n, ok := f.cache.get(offset); ok {
		return n, nil
	}

	buf := make([]byte, pageSize)

	if _, err := f.file.ReadAt(buf, int64(offset)); err != nil && err != io.EOF {
		return nil, err
	}

	n := &btreeNode{}
	switch buf[0] {
	case InternalNode:
		n.isLeaf = false
	case LeafNode:
		n.isLeaf = true
	default:
		panic("invalid node type value")
	}

	if err := n.decode(bytes.NewBuffer(buf)); err != nil {
		return nil, err
	}

	if err := f.setCache(n.getFileOffset(), n); err != nil {
		return nil, err
	}

	return n, nil
}

func (f *fileStore) save() error {
	writer := bytes.NewBuffer(make([]byte, 0, 20))

	if err := binary.Write(writer, binary.LittleEndian, f.lastKey); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, f.pageTableRoot); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, f.nextFreeOffset); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, f._nextLSN); err != nil {
		return err
	}
	if _, err := f.file.WriteAt(writer.Bytes(), 0); err != nil {
		return err
	}

	return nil
}

func (f *fileStore) open() error {
	if err := binary.Read(f.file, binary.LittleEndian, &f.lastKey); err != nil {
		return err
	}
	if err := binary.Read(f.file, binary.LittleEndian, &f.pageTableRoot); err != nil {
		return err
	}
	if err := binary.Read(f.file, binary.LittleEndian, &f.nextFreeOffset); err != nil {
		return err
	}
	if err := binary.Read(f.file, binary.LittleEndian, &f._nextLSN); err != nil {
		return err
	}
	return nil
}

func (f *fileStore) flushPages() error {
	f.lockExclusive()
	defer f.unlockExclusive()
	for _, v := range f.cache.cache {
		node := v.Value.(*cacheEntry).val
		if !node.isDirty() {
			continue
		}
		if err := f.update(node); err != nil {
			return err
		}
		node.markClean()
	}
	return f.save()
}

func (f *fileStore) setCache(key any, val *btreeNode) error {
	if !f.cache.set(key, val) {
		return ErrLRUCacheFull
	}
	return nil
}

func (f *fileStore) nextLSN() uint64 {
	return f._nextLSN
}

func (f *fileStore) incrLSN() {
	f._nextLSN++
}
