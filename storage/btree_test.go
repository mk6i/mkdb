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

	if err := bt.store.append(rootPg); err != nil {
		t.Fatal(err)
	}

	bt.setRoot(rootPg)

	tbl := []struct {
		key uint32
		val []byte
	}{
		{key: 2, val: []byte("bounjour")},
		{key: 1, val: []byte("hello")},
		{key: 5, val: []byte("chien")},
		{key: 11, val: []byte("lumber")},
		{key: 4, val: []byte("chat")},
		{key: 10, val: []byte("saw")},
		{key: 7, val: []byte("nail")},
		{key: 8, val: []byte("screwdriver")},
		{key: 9, val: []byte("screw")},
		{key: 3, val: []byte("hallo")},
		{key: 6, val: []byte("hammer")},
	}

	for _, expect := range tbl {
		err := bt.insertKey(expect.key, expect.val)
		if err != nil {
			t.Fatalf("got insertion error for %s: %s", expect.val, err.Error())
		}
	}

	for _, expect := range tbl {
		val, err := bt.find(expect.key)
		if err != nil {
			t.Fatalf("got retrieval error for %d: %s", expect.key, err.Error())
		}
		if !bytes.Equal(val, expect.val) {
			t.Errorf("value mismatch for key %d. got %s, expected %s", expect.key, val, expect.val)
		}
	}

	tblSorted := []struct {
		key uint32
		val []byte
	}{
		{key: 1, val: []byte("hello")},
		{key: 2, val: []byte("bounjour")},
		{key: 3, val: []byte("hallo")},
		{key: 4, val: []byte("chat")},
		{key: 5, val: []byte("chien")},
		{key: 6, val: []byte("hammer")},
		{key: 7, val: []byte("nail")},
		{key: 8, val: []byte("screwdriver")},
		{key: 9, val: []byte("screw")},
		{key: 10, val: []byte("saw")},
		{key: 11, val: []byte("lumber")},
	}

	ch, err := bt.scanRight()
	if err != nil {
		t.Fatal(err)
	}

	for _, expect := range tblSorted {
		val, ok := <-ch
		if !ok {
			t.Fatalf("channel is unexpectedly empty")
		}
		if !bytes.Equal(val.valueBytes, expect.val) {
			t.Errorf("value mismatch for key %d. got %s, expected %s", expect.key, val.valueBytes, expect.val)
		}
	}

	ch, err = bt.scanLeft()
	if err != nil {
		t.Fatal(err)
	}

	for i := len(tblSorted) - 1; i >= 0; i-- {
		val, ok := <-ch
		if !ok {
			t.Fatalf("channel is unexpectedly empty")
		}
		if !bytes.Equal(val.valueBytes, tblSorted[i].val) {
			t.Errorf("value mismatch for key %d. got %s, expected %s", tblSorted[i].key, val.valueBytes, tblSorted[i].val)
		}
	}
}
