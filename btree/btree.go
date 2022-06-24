package btree

import (
	"fmt"
)

type BTree struct {
	store
}

func (b *BTree) insert(value []byte) (uint32, error) {
	nextKey := b.store.getLastKey() + 1
	offset, err := b.insertKey(nextKey, value)
	b.store.incrementLastKey()
	return offset, err
}

func (b *BTree) insertKey(key uint32, value []byte) (uint32, error) {
	pg := b.store.getRoot()
	b.insertHelper(nil, pg, key, value)
	return key, nil
}

func (b *BTree) insertHelper(parent *page, pg *page, key uint32, value []byte) {

	if pg.cellType == KeyCell {

		offset, found := pg.findCellOffsetByKey(key)
		if found {
			panic(fmt.Sprintf("error appending cell: %d", key))
		}

		var pgID uint32
		if offset == len(pg.offsets) {
			pgID = pg.rightOffset
		} else {
			pgID = pg.cells[pg.offsets[offset]].(*keyCell).pageID
		}

		childPg, _ := b.store.fetch(pgID)
		b.insertHelper(pg, childPg, key, value)

		if pg.isFull(b.store.getBranchFactor()) {
			newPg := &page{}
			b.store.append(newPg)
			newKey := pg.split(newPg)
			if parent == nil {
				parent = &page{
					cellType: KeyCell,
				}
				b.store.append(parent)
				b.store.setRoot(parent)
				parent.setRightMostKey(newPg.pageID)
				parent.appendKeyCell(newKey, pg.pageID)
			} else {
				parent.appendKeyCell(newKey, parent.rightOffset)
				parent.setRightMostKey(newPg.pageID)
			}
		}
	} else {
		offset, found := pg.findCellOffsetByKey(key)
		if found {
			panic(fmt.Sprintf("key already exists at offset %d", offset))
		}
		err := pg.insertCell(uint32(offset), key, value)
		if err != nil {
			panic(fmt.Sprintf("error appending cell: %s", err.Error()))
		}
		if pg.isFull(b.store.getBranchFactor()) {
			newPg := &page{
				cellType: KeyValueCell,
			}
			b.store.append(newPg)
			newKey := pg.split(newPg)

			oldRSibPageID := pg.rSibPageID
			pg.hasRSib = true
			pg.rSibPageID = newPg.pageID
			newPg.hasLSib = true
			newPg.lSibPageID = pg.pageID

			if parent == nil {
				parent = &page{
					cellType: KeyCell,
				}
				b.store.append(parent)
				b.store.setRoot(parent)
				parent.setRightMostKey(newPg.pageID)
				parent.appendKeyCell(newKey, pg.pageID)
			} else {
				if newKey > parent.getRightmostKey() {
					parent.appendKeyCell(newKey, parent.rightOffset)
					parent.setRightMostKey(newPg.pageID)
				} else {
					newPg.hasRSib = true
					newPg.rSibPageID = oldRSibPageID

					offset, found := parent.findCellOffsetByKey(newKey)
					if found {
						panic(fmt.Sprintf("error appending cell: %d", key))
					}
					parent.insertKeyCell(uint32(offset), newKey, newPg.pageID)

					// update old right sibling's left pointer
					rightSib, _ := b.store.fetch(oldRSibPageID)
					rightSib.lSibPageID = newPg.pageID
				}
			}
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

	offset, found := pg.findCellOffsetByKey(key)
	if !found {
		return nil, nil
	}
	cell := pg.cells[pg.offsets[offset]].(*keyValueCell)
	return cell.valueBytes, nil
}

func (b *BTree) scanRight() chan *keyValueCell {

	// find left-most leaf node
	node := b.getRoot()
	for node.cellType == KeyCell {
		pgID := node.cells[node.offsets[0]].(*keyCell).pageID
		var err error
		node, err = b.store.fetch(pgID)
		if err != nil {
			panic(fmt.Sprintf("error fetching page during table scan: %s", err.Error()))
		}
	}

	ch := make(chan *keyValueCell)

	go func(pg *page) {
		for {
			for _, offset := range pg.offsets {
				ch <- pg.cells[offset].(*keyValueCell)
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
		close(ch)
	}(node)

	return ch
}

func (b *BTree) scanLeft() chan *keyValueCell {

	// find right-most leaf node
	node := b.getRoot()
	for node.cellType == KeyCell {
		var err error
		node, err = b.store.fetch(node.rightOffset)
		if err != nil {
			panic(fmt.Sprintf("error fetching page during table scan: %s", err.Error()))
		}
	}

	ch := make(chan *keyValueCell)

	go func(pg *page) {
		for {
			for i := len(pg.offsets) - 1; i >= 0; i-- {
				ch <- pg.cells[pg.offsets[i]].(*keyValueCell)
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
		close(ch)
	}(node)

	return ch
}
