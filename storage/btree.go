package btree

import (
	"errors"
	"fmt"
)

var errKeyAlreadyExists = errors.New("record already exists")

// ScanAction signals whether the scan iterator can continue after processing
// an iterator callack.
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

		var pgID uint32
		if offset == len(pg.offsets) {
			pgID = pg.rightOffset
		} else {
			pgID = pg.cells[pg.offsets[offset]].(*keyCell).pageID
		}

		childPg, err := b.store.fetch(pgID)
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
				parent.setRightMostKey(newPg.pageID)
				if err := parent.appendKeyCell(newKey, pg.pageID); err != nil {
					return err
				}
			} else {
				if err := parent.appendKeyCell(newKey, parent.rightOffset); err != nil {
					return err
				}
				parent.setRightMostKey(newPg.pageID)
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

			oldRSibPageID := pg.rSibPageID
			pg.hasRSib = true
			pg.rSibPageID = newPg.pageID
			newPg.hasLSib = true
			newPg.lSibPageID = pg.pageID

			if parent == nil {
				parent = &page{
					cellType: KeyCell,
				}
				if err := b.store.append(parent); err != nil {
					return err
				}
				b.store.setRoot(parent)
				parent.setRightMostKey(newPg.pageID)
				if err := parent.appendKeyCell(newKey, pg.pageID); err != nil {
					return err
				}
			} else {
				if newKey > parent.getRightmostKey() {
					if err := parent.appendKeyCell(newKey, parent.rightOffset); err != nil {
						return err
					}
					parent.setRightMostKey(newPg.pageID)
				} else {
					newPg.hasRSib = true
					newPg.rSibPageID = oldRSibPageID

					offset, found := parent.findCellOffsetByKey(newKey)
					if found {
						return fmt.Errorf("%w for key: %d", errKeyAlreadyExists, newKey)
					}
					if err := parent.insertKeyCell(uint32(offset), newKey, newPg.pageID); err != nil {
						return err
					}

					// update old right sibling's left pointer
					rightSib, err := b.store.fetch(oldRSibPageID)
					if err != nil {
						return err
					}
					rightSib.lSibPageID = newPg.pageID
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
				var pgID uint32
				if i == len(pg.offsets) {
					pgID = pg.rightOffset
				} else {
					pgID = pg.cells[pg.offsets[i]].(*keyCell).pageID
				}
				var err error
				pg, err = b.store.fetch(pgID)
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
		pgID := pg.cells[pg.offsets[0]].(*keyCell).pageID
		var err error
		pg, err = b.store.fetch(pgID)
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
			pg, err = b.store.fetch(pg.rSibPageID)
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
			pg, err = b.store.fetch(pg.lSibPageID)
			if err != nil {
				panic(fmt.Sprintf("error fetching page during table scan: %s", err.Error()))
			}
		} else {
			break
		}
	}

	return nil
}
