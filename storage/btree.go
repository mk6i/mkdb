package storage

import (
	"errors"
	"fmt"
)

var errKeyAlreadyExists = errors.New("record already exists")

// ScanAction signals whether the scan iterator can continue after processing
// an iterator callback.
type ScanAction bool

var (
	KeepScanning = ScanAction(true)
	StopScanning = ScanAction(false)
)

type BTree struct {
	store
	rootOffset uint64
}

func (b *BTree) getRoot() (*btreeNode, error) {
	return b.store.fetch(b.rootOffset)
}

func (b *BTree) setRoot(node *btreeNode) {
	b.rootOffset = node.getFileOffset()
}

func (b *BTree) insert(value []byte) (uint32, uint64, error) {
	nextKey := b.store.getLastKey() + 1
	nextLSN := b.store.nextLSN()
	err := b.insertKey(nextKey, nextLSN, value)
	if err := b.store.incrementLastKey(); err != nil {
		return 0, nextLSN, err
	}
	b.store.incrLSN()
	return nextKey, nextLSN, err
}

func (b *BTree) insertKey(key uint32, nextLSN uint64, value []byte) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}
	if pg.isLeaf {
		return b.insertLeaf(nil, pg, key, nextLSN, value)
	}
	return b.insertInternal(nil, pg, key, nextLSN, value)
}

func (b *BTree) insertInternal(parent *btreeNode, curNode *btreeNode, key uint32, nextLSN uint64, value []byte) error {
	offset, found := curNode.findCellOffsetByKey(key)
	if found {
		return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, key)
	}

	var fileOffset uint64
	if offset == len(curNode.offsets) {
		fileOffset = curNode.rightOffset
	} else {
		fileOffset = curNode.internalCells[curNode.offsets[offset]].fileOffset
	}

	childPg, err := b.store.fetch(fileOffset)
	if err != nil {
		return err
	}

	if childPg.isLeaf {
		if err := b.insertLeaf(curNode, childPg, key, nextLSN, value); err != nil {
			return err
		}
	} else {
		if err := b.insertInternal(curNode, childPg, key, nextLSN, value); err != nil {
			return err
		}
	}

	if !curNode.isFull() {
		return nil
	}

	newPg := &btreeNode{}
	if err := b.store.append(newPg); err != nil {
		return err
	}

	newKey, err := curNode.split(newPg)
	if err != nil {
		return err
	}

	if parent == nil {
		parent = &btreeNode{}
		if err := b.store.append(parent); err != nil {
			return err
		}
		b.setRoot(parent)
		parent.setRightMostKey(newPg.fileOffset)
		if err := parent.appendInternalCell(newKey, curNode.fileOffset); err != nil {
			return err
		}
	} else {
		if err := parent.appendInternalCell(newKey, parent.rightOffset); err != nil {
			return err
		}
		parent.setRightMostKey(newPg.fileOffset)
	}

	newPg.markDirty(nextLSN)
	parent.markDirty(nextLSN)

	if parent != nil {
		parent.markDirty(nextLSN)
	}

	return nil
}

func (b *BTree) insertLeaf(parent *btreeNode, curNode *btreeNode, key uint32, nextLSN uint64, value []byte) error {
	offset, found := curNode.findCellOffsetByKey(key)
	if found {
		return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, key)
	}

	if err := curNode.insertLeafCell(uint32(offset), key, value); err != nil {
		return err
	}

	curNode.markDirty(nextLSN)

	if !curNode.isFull() {
		return nil
	}

	newPg := &btreeNode{isLeaf: true}
	if err := b.store.append(newPg); err != nil {
		return err
	}

	newKey, err := curNode.split(newPg)
	if err != nil {
		return err
	}

	oldRSibFileOffset := curNode.rSibFileOffset
	curNode.hasRSib = true
	curNode.rSibFileOffset = newPg.fileOffset
	newPg.hasLSib = true
	newPg.lSibFileOffset = curNode.fileOffset

	if parent == nil {
		parent = &btreeNode{}
		if err := b.store.append(parent); err != nil {
			return err
		}
		b.setRoot(parent)
		parent.setRightMostKey(newPg.fileOffset)
		if err := parent.appendInternalCell(newKey, curNode.fileOffset); err != nil {
			return err
		}
	} else {
		if newKey > parent.getRightmostKey() {
			if err := parent.appendInternalCell(newKey, parent.rightOffset); err != nil {
				return err
			}
			parent.setRightMostKey(newPg.fileOffset)
		} else {
			newPg.hasRSib = true
			newPg.rSibFileOffset = oldRSibFileOffset

			offset, found := parent.findCellOffsetByKey(newKey)
			if found {
				return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, newKey)
			}
			if err := parent.insertInternalCell(uint32(offset), newKey, newPg.fileOffset); err != nil {
				return err
			}

			// update old right sibling's left pointer
			rightSib, err := b.store.fetch(oldRSibFileOffset)
			if err != nil {
				return err
			}
			rightSib.lSibFileOffset = newPg.fileOffset
			rightSib.markDirty(nextLSN)
		}
	}

	newPg.markDirty(nextLSN)
	curNode.markDirty(nextLSN)

	if parent != nil {
		parent.markDirty(nextLSN)
	}

	return nil
}

func (b *BTree) findCell(key uint32) (*leafCell, error) {

	pg, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	for !pg.isLeaf {
		// todo: replace with binary search
		for i := 0; i <= len(pg.offsets); i++ {
			if i == len(pg.offsets) || key < pg.cellKey(pg.offsets[i]) {
				var fileOffset uint64
				if i == len(pg.offsets) {
					fileOffset = pg.rightOffset
				} else {
					fileOffset = pg.internalCells[pg.offsets[i]].fileOffset
				}
				var err error
				pg, err = b.store.fetch(fileOffset)
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}

	offset, found := pg.findCellOffsetByKey(key)
	if !found {
		return nil, nil
	}

	cell := pg.leafCells[pg.offsets[offset]]
	if cell.deleted {
		return nil, nil
	}

	// fixme: setting parent node is kind of yucky
	cell.pg = pg
	return cell, nil
}

func (b *BTree) scanRight(f func(kv *leafCell) (ScanAction, error)) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}

	// find left-most leaf node
	// todo: store reference to left-mode leaf node
	for !pg.isLeaf {
		fileOffset := pg.internalCells[pg.offsets[0]].fileOffset
		var err error
		pg, err = b.store.fetch(fileOffset)
		if err != nil {
			return fmt.Errorf("table scan error: %w", err)
		}
	}

	for {
		for _, offset := range pg.offsets {
			cell := pg.leafCells[offset]
			if cell.deleted {
				continue
			}
			cell.pg = pg
			nextScan, err := f(cell)
			if err != nil {
				return err
			}
			if nextScan == StopScanning {
				return nil
			}
		}
		if pg.hasRSib {
			var err error
			pg, err = b.store.fetch(pg.rSibFileOffset)
			if err != nil {
				return fmt.Errorf("table scan error: %w", err)
			}
		} else {
			break
		}
	}

	return nil
}

func (b *BTree) scanLeft(f func(kv *leafCell) (ScanAction, error)) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}

	// find right-most leaf node
	// todo: store reference to right-mode leaf node
	for !pg.isLeaf {
		var err error
		pg, err = b.store.fetch(pg.rightOffset)
		if err != nil {
			return fmt.Errorf("table scan error: %w", err)
		}
	}

	for {
		for offset := len(pg.offsets) - 1; offset >= 0; offset-- {
			cell := pg.leafCells[pg.offsets[offset]]
			if cell.deleted {
				continue
			}
			cell.pg = pg
			nextScan, err := f(cell)
			if err != nil {
				return err
			}
			if nextScan == StopScanning {
				return nil
			}
		}
		if pg.hasLSib {
			var err error
			pg, err = b.store.fetch(pg.lSibFileOffset)
			if err != nil {
				return fmt.Errorf("table scan error: %w", err)
			}
		} else {
			break
		}
	}

	return nil
}
