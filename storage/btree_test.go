package btree

import (
	"bytes"
	"testing"
)

func TestBTree(t *testing.T) {

	bt := &BTree{&memoryStore{}}
	bt.append(&page{
		cellType: KeyValueCell,
	})

	tbl := []struct {
		key uint32
		val []byte
	}{
		{key: 1, val: []byte("hello")},
		{key: 2, val: []byte("bounjour")},
		{key: 3, val: []byte("hallo")},
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
