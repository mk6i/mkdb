package btree

type BTree struct {
	store
}

func (b *BTree) insert(p *page) (uint32, error) {
	return b.store.append(p)
}

func (b *BTree) find(key uint32) (*page, error) {
	return b.store.fetch(uint16(key))
}
