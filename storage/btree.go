package btree

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
	pg, err := b.store.getRoot()
	if err != nil {
		return err
	}
	err = b.insertHelper(nil, pg, key, value)
	return err
}

func (b *BTree) insertHelper(parent *page, pg *page, key uint32, value []byte) error {

	if pg.cellType == KeyCell {

		offset, found := pg.findCellOffsetByKey(key)
		if found {
			return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, key)
		}

		var fileOffset uint64
		if offset == len(pg.offsets) {
			fileOffset = pg.rightOffset
		} else {
			fileOffset = pg.cells[pg.offsets[offset]].(*keyCell).fileOffset
		}

		childPg, err := b.store.fetch(fileOffset)
		if err != nil {
			return err
		}

		if err := b.insertHelper(pg, childPg, key, value); err != nil {
			return err
		}

		if pg.isFull() {
			newPg := &page{}
			if err := b.store.append(newPg); err != nil {
				return err
			}
			newKey, err := pg.split(newPg)
			if err != nil {
				return err
			}
			if parent == nil {
				parent = &page{
					cellType: KeyCell,
				}
				if err := b.store.append(parent); err != nil {
					return err
				}
				b.store.setRoot(parent)
				parent.setRightMostKey(newPg.fileOffset)
				if err := parent.appendKeyCell(newKey, pg.fileOffset); err != nil {
					return err
				}
			} else {
				if err := parent.appendKeyCell(newKey, parent.rightOffset); err != nil {
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
	} else {
		offset, found := pg.findCellOffsetByKey(key)
		if found {
			return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, key)
		}
		if err := pg.insertCell(uint32(offset), key, value); err != nil {
			return err
		}
		if err := b.store.update(pg); err != nil {
			return err
		}

		if pg.isFull() {
			newPg := &page{
				cellType: KeyValueCell,
			}
			if err := b.store.append(newPg); err != nil {
				return err
			}
			newKey, err := pg.split(newPg)
			if err != nil {
				return err
			}

			oldRSibFileOffset := pg.rSibFileOffset
			pg.hasRSib = true
			pg.rSibFileOffset = newPg.fileOffset
			newPg.hasLSib = true
			newPg.lSibFileOffset = pg.fileOffset

			if parent == nil {
				parent = &page{
					cellType: KeyCell,
				}
				if err := b.store.append(parent); err != nil {
					return err
				}
				b.store.setRoot(parent)
				parent.setRightMostKey(newPg.fileOffset)
				if err := parent.appendKeyCell(newKey, pg.fileOffset); err != nil {
					return err
				}
			} else {
				if newKey > parent.getRightmostKey() {
					if err := parent.appendKeyCell(newKey, parent.rightOffset); err != nil {
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
					if err := parent.insertKeyCell(uint32(offset), newKey, newPg.fileOffset); err != nil {
						return err
					}

					// update old right sibling's left pointer
					rightSib, err := b.store.fetch(oldRSibFileOffset)
					if err != nil {
						return err
					}
					rightSib.lSibFileOffset = newPg.fileOffset
					if err := b.store.update(rightSib); err != nil {
						return err
					}
				}
			}
			if err := b.store.update(newPg); err != nil {
				return err
			}
		}
		if err := b.store.update(pg); err != nil {
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

func (b *BTree) find(key uint32) ([]byte, error) {

	pg, err := b.store.getRoot()
	if err != nil {
		return nil, err
	}

	for pg.cellType == KeyCell {
		for i := 0; i <= len(pg.offsets); i++ {
			if i == len(pg.offsets) || key < pg.cellKey(pg.offsets[i]) {
				var fileOffset uint64
				if i == len(pg.offsets) {
					fileOffset = pg.rightOffset
				} else {
					fileOffset = pg.cells[pg.offsets[i]].(*keyCell).fileOffset
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
	cell := pg.cells[pg.offsets[offset]].(*keyValueCell)
	return cell.valueBytes, nil
}

func (b *BTree) scanRight(f func(kv *keyValueCell) (ScanAction, error)) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}

	// find left-most leaf node
	for pg.cellType == KeyCell {
		fileOffset := pg.cells[pg.offsets[0]].(*keyCell).fileOffset
		var err error
		pg, err = b.store.fetch(fileOffset)
		if err != nil {
			panic(fmt.Sprintf("error fetching page during table scan: %s", err.Error()))
		}
	}

	for {
		for _, offset := range pg.offsets {
			kvCell := pg.cells[offset].(*keyValueCell)
			kvCell.pg = pg
			nextScan, err := f(kvCell)
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
				panic(fmt.Sprintf("error fetching page during table scan: %s", err.Error()))
			}
		} else {
			break
		}
	}

	return nil
}

func (b *BTree) scanLeft(f func(kv *keyValueCell) (ScanAction, error)) error {
	pg, err := b.getRoot()
	if err != nil {
		return err
	}

	// find right-most leaf node
	for pg.cellType == KeyCell {
		var err error
		pg, err = b.store.fetch(pg.rightOffset)
		if err != nil {
			panic(fmt.Sprintf("error fetching page during table scan: %s", err.Error()))
		}
	}

	for {
		for offset := len(pg.offsets) - 1; offset >= 0; offset-- {
			kvCell := pg.cells[pg.offsets[offset]].(*keyValueCell)
			kvCell.pg = pg
			nextScan, err := f(kvCell)
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
				panic(fmt.Sprintf("error fetching page during table scan: %s", err.Error()))
			}
		} else {
			break
		}
	}

	return nil
}
