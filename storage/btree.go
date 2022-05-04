package btree

type BTree struct {
	store
}

func (b *BTree) insert(value []byte) (uint32, error) {

	nextKey := b.store.getLastKey() + 1

	// always insert into root node for now
	pg, err := b.store.fetch(0)
	if err != nil {
		return 0, err
	}

	err = pg.appendCell(nextKey, value)
	if err != nil {
		return 0, err
	}

	b.store.incrementLastKey()

	return nextKey, nil
}

func (b *BTree) find(key uint32) ([]byte, error) {

	pg, err := b.store.fetch(0)
	if err != nil {
		return nil, err
	}

	offset, found := pg.findCellOffsetByKey(key)
	if !found {
		return nil, nil
	}

	val := pg.getCellValue(offset)

	return val, nil
}
