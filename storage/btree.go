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

func (b *BTree) getRoot() (btreeNode, error) {
	return b.store.fetch(b.rootOffset)
}

func (b *BTree) setRoot(node btreeNode) {
	b.rootOffset = node.getFileOffset()
}

func (b *BTree) insert(value []byte) (uint32, error) {
	nextKey := b.store.getLastKey() + 1
	err := b.insertKey(nextKey, value)
	if err := b.store.incrementLastKey(); err != nil {
		return 0, err
	}
	return nextKey, err
}

func (b *BTree) insertKey(key uint32, value []byte) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}
	err = b.insertHelper(nil, pg, key, value)
	return err
}

func (b *BTree) insertHelper(parent *internalNode, node btreeNode, key uint32, value []byte) error {

	switch node := node.(type) {
	case *internalNode:
		offset, found := node.findCellOffsetByKey(key)
		if found {
			return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, key)
		}

		var fileOffset uint64
		if offset == len(node.offsets) {
			fileOffset = node.rightOffset
		} else {
			fileOffset = node.cells[node.offsets[offset]].fileOffset
		}

		childPg, err := b.store.fetch(fileOffset)
		if err != nil {
			return err
		}

		if err := b.insertHelper(node, childPg, key, value); err != nil {
			return err
		}

		if node.isFull() {
			newPg := &internalNode{}
			if err := b.store.append(newPg); err != nil {
				return err
			}
			newKey, err := node.split(newPg)
			if err != nil {
				return err
			}
			if parent == nil {
				parent = &internalNode{}
				if err := b.store.append(parent); err != nil {
					return err
				}
				b.setRoot(parent)
				parent.setRightMostKey(newPg.fileOffset)
				if err := parent.appendCell(newKey, node.fileOffset); err != nil {
					return err
				}
			} else {
				if err := parent.appendCell(newKey, parent.rightOffset); err != nil {
					return err
				}
				parent.setRightMostKey(newPg.fileOffset)
			}
			if err := b.store.update(newPg); err != nil {
				return err
			}
			if err := b.store.update(parent); err != nil {
				return err
			}
		}
		if parent != nil {
			if err := b.store.update(parent); err != nil {
				return err
			}
		}
	case *leafNode:
		offset, found := node.findCellOffsetByKey(key)
		if found {
			return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, key)
		}
		if err := node.insertCell(uint32(offset), key, value); err != nil {
			return err
		}
		if err := b.store.update(node); err != nil {
			return err
		}

		if node.isFull() {
			newPg := &leafNode{}
			if err := b.store.append(newPg); err != nil {
				return err
			}
			newKey, err := node.split(newPg)
			if err != nil {
				return err
			}

			oldRSibFileOffset := node.rSibFileOffset
			node.hasRSib = true
			node.rSibFileOffset = newPg.fileOffset
			newPg.hasLSib = true
			newPg.lSibFileOffset = node.fileOffset

			if parent == nil {
				parent = &internalNode{}
				if err := b.store.append(parent); err != nil {
					return err
				}
				b.setRoot(parent)
				parent.setRightMostKey(newPg.fileOffset)
				if err := parent.appendCell(newKey, node.fileOffset); err != nil {
					return err
				}
			} else {
				if newKey > parent.getRightmostKey() {
					if err := parent.appendCell(newKey, parent.rightOffset); err != nil {
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
					if err := parent.insertCell(uint32(offset), newKey, newPg.fileOffset); err != nil {
						return err
					}

					// update old right sibling's left pointer
					rightSib, err := b.store.fetch(oldRSibFileOffset)
					if err != nil {
						return err
					}
					rightSib.(*leafNode).lSibFileOffset = newPg.fileOffset
					if err := b.store.update(rightSib); err != nil {
						return err
					}
				}
			}
			if err := b.store.update(newPg); err != nil {
				return err
			}
		}
		if err := b.store.update(node); err != nil {
			return err
		}
		if parent != nil {
			if err := b.store.update(parent); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BTree) findCell(key uint32) (*leafNodeCell, error) {

	pg, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	for {
		if _, ok := pg.(*internalNode); !ok {
			break
		}
		// todo: replace with binary search
		for i := 0; i <= len(pg.(*internalNode).offsets); i++ {
			if i == len(pg.(*internalNode).offsets) || key < pg.(*internalNode).cellKey(pg.(*internalNode).offsets[i]) {
				var fileOffset uint64
				if i == len(pg.(*internalNode).offsets) {
					fileOffset = pg.(*internalNode).rightOffset
				} else {
					fileOffset = pg.(*internalNode).cells[pg.(*internalNode).offsets[i]].fileOffset
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

	offset, found := pg.(*leafNode).findCellOffsetByKey(key)
	if !found {
		return nil, nil
	}

	cell := pg.(*leafNode).cells[pg.(*leafNode).offsets[offset]]
	if cell.deleted {
		return nil, nil
	}

	// fixme: setting parent node is kind of yucky
	cell.pg = pg
	return cell, nil
}

func (b *BTree) scanRight(f func(kv *leafNodeCell) (ScanAction, error)) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}

	// find left-most leaf node
	// todo: store reference to left-mode leaf node
	for {
		if _, ok := pg.(*internalNode); !ok {
			break
		}
		fileOffset := pg.(*internalNode).cells[pg.(*internalNode).offsets[0]].fileOffset
		var err error
		pg, err = b.store.fetch(fileOffset)
		if err != nil {
			return fmt.Errorf("table scan error: %w", err)
		}
	}

	for {
		for _, offset := range pg.(*leafNode).offsets {
			cell := pg.(*leafNode).cells[offset]
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
		if pg.(*leafNode).hasRSib {
			var err error
			pg, err = b.store.fetch(pg.(*leafNode).rSibFileOffset)
			if err != nil {
				return fmt.Errorf("table scan error: %w", err)
			}
		} else {
			break
		}
	}

	return nil
}

func (b *BTree) scanLeft(f func(kv *leafNodeCell) (ScanAction, error)) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}

	// find right-most leaf node
	// todo: store reference to right-mode leaf node
	for {
		if _, ok := pg.(*internalNode); !ok {
			break
		}
		var err error
		pg, err = b.store.fetch(pg.(*internalNode).rightOffset)
		if err != nil {
			return fmt.Errorf("table scan error: %w", err)
		}
	}

	for {
		for offset := len(pg.(*leafNode).offsets) - 1; offset >= 0; offset-- {
			cell := pg.(*leafNode).cells[pg.(*leafNode).offsets[offset]]
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
		if pg.(*leafNode).hasLSib {
			var err error
			pg, err = b.store.fetch(pg.(*leafNode).lSibFileOffset)
			if err != nil {
				return fmt.Errorf("table scan error: %w", err)
			}
		} else {
			break
		}
	}

	return nil
}
