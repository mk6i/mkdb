package btree

import "testing"

func TestBTree(t *testing.T) {

	bt := &BTree{&memoryStore{}}

	pg := &page{}

	offset, err := bt.insert(pg)
	if err != nil {
		t.Errorf("failed to insert %s", err.Error())
	}

	res, err := bt.find(offset)
	if err != nil {
		t.Errorf("failed to find %s", err.Error())
	}

	if res == nil {
		t.Errorf("did not find page that was inserted")
	} else if res != pg {
		t.Errorf("page result was not the same page that was inserted")
	}
}
