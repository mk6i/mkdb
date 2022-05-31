package btree

import (
	"bytes"
	"testing"
)

func TestBTree(t *testing.T) {

	rootPg := &page{
		cellType: KeyValueCell,
	}

	bt := &BTree{
		store: &memoryStore{
			branchFactor: 4,
		},
	}

	bt.store.append(rootPg)
	bt.setRoot(rootPg)

	tbl := []struct {
		key uint32
		val []byte
	}{
		{key: 1, val: []byte("hello")},
		{key: 2, val: []byte("bounjour")},
		{key: 3, val: []byte("hallo")},
		{key: 4, val: []byte("chat")},
		{key: 5, val: []byte("chien")},
	}

	for _, expect := range tbl {
		key, err := bt.insert(expect.val)
		if err != nil {
			t.Errorf("got insertion error for %s: %s", expect.val, err.Error())
		}
		if key != expect.key {
			t.Errorf("expected primary key %d, got %d", uint32(expect.key), key)
		}
	}

	for _, expect := range tbl {
		val, err := bt.find(expect.key)
		if err != nil {
			t.Errorf("got retrieval error for %d: %s", expect.key, err.Error())
		}
		if !bytes.Equal(val, expect.val) {
			t.Errorf("value mismatch for key %d. got %s, expected %s", expect.key, val, expect.val)
		}
	}
}
