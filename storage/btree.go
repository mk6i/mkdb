package btree

import (
	"fmt"
)

type BTree struct {
	store
}

func (b *BTree) insert(value []byte) (uint32, error) {

	nextKey := b.store.getLastKey() + 1
	pg := b.store.getRoot()

	b.insertHelper(nil, pg, nextKey, value)
	b.store.incrementLastKey()

	return nextKey, nil
}

func (b *BTree) insertHelper(parent *page, pg *page, key uint32, value []byte) {

	if pg.cellType == KeyCell {
		for i := 0; i <= len(pg.offsets); i++ {
			if i == len(pg.offsets) || key < pg.cellKey(pg.offsets[i]) {

				var pgID uint32
				if i == len(pg.offsets) {
					pgID = pg.rightOffset
				} else {
					pgID = pg.cells[pg.offsets[i]].(*keyCell).pageID
				}

				childPg, _ := b.store.fetch(pgID)
				b.insertHelper(pg, childPg, key, value)

				if pg.isFull(b.store.getBranchFactor()) {
					newPg := &page{}
					newKey := pg.split(newPg)
					offset, err := b.store.append(newPg)
					if err != nil {
						panic(fmt.Sprintf("error appending page to store: %s", err.Error()))
					}
					if childPg.cellType == KeyCell {
						parent.appendKeyCell(newKey, offset)
					} else {
						parent.appendKeyCell(newKey, offset)
					}
				}
				break
			}
		}
	} else {
		err := pg.appendCell(key, value)
		if err != nil {
			panic(fmt.Sprintf("error appending cell: %s", err.Error()))
		}
		if pg.isFull(b.store.getBranchFactor()) {
			newPg := &page{
				cellType: KeyValueCell,
			}
			newKey := pg.split(newPg)
			b.store.append(newPg)
			if parent == nil {
				parent = &page{
					cellType: KeyCell,
				}
				b.store.setRoot(parent)
				b.store.append(parent)
				parent.setRightMostKey(newPg.pageID)
			}
			parent.appendKeyCell(newKey, pg.pageID)
		}
	}
}
func (b *BTree) find(key uint32) ([]byte, error) {

	pg := b.store.getRoot()

	for pg.cellType == KeyCell {
		for i := 0; i <= len(pg.offsets); i++ {
			if i == len(pg.offsets) || key < pg.cellKey(pg.offsets[i]) {
				var pgID uint32
				if i == len(pg.offsets) {
					pgID = pg.rightOffset
				} else {
					pgID = pg.cells[pg.offsets[i]].(*keyCell).pageID
				}
				pg, _ = b.store.fetch(pgID)
				break
			}
		}
	}

	for i := 0; i < len(pg.offsets); i++ {
		if pg.cellKey(pg.offsets[i]) == key {
			cell := pg.cells[i].(*keyValueCell)
			return cell.valueBytes, nil
		}
	}

	return nil, nil
}
