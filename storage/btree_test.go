package storage

import (
	"bytes"
	"testing"
)

func TestBTree(t *testing.T) {

	rootPg := &btreeNode{isLeaf: true}

	bt := &BTree{
		store: &memoryStore{},
	}

	if err := bt.store.append(rootPg); err != nil {
		t.Fatal(err)
	}

	bt.setRoot(rootPg)

	tbl := []struct {
		key uint32
		val []byte
	}{
		{key: 2, val: []byte("bonjour")},
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
		err := bt.insertKey(expect.key, 0, expect.val)
		if err != nil {
			t.Fatalf("got insertion error for %s: %s", expect.val, err.Error())
		}
	}

	for _, expect := range tbl {
		val, err := bt.findCell(expect.key)
		if err != nil {
			t.Fatalf("got retrieval error for %d: %s", expect.key, err.Error())
		}
		if !bytes.Equal(val.valueBytes, expect.val) {
			t.Errorf("value mismatch for key %d. got %s, expected %s", expect.key, val.valueBytes, expect.val)
		}
	}

	tblSorted := []struct {
		key uint32
		val []byte
	}{
		{key: 1, val: []byte("hello")},
		{key: 2, val: []byte("bonjour")},
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

	idx := 0

	err := bt.scanRight(func(val *leafCell) (ScanAction, error) {
		expect := tblSorted[idx]
		if !bytes.Equal(val.valueBytes, expect.val) {
			t.Errorf("value mismatch for key %d. got %s, expected %s", expect.key, val.valueBytes, expect.val)
			return StopScanning, nil
		}
		idx++
		return KeepScanning, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	idx = len(tblSorted) - 1
	err = bt.scanLeft(func(val *leafCell) (ScanAction, error) {
		expect := tblSorted[idx]
		if !bytes.Equal(val.valueBytes, expect.val) {
			t.Errorf("value mismatch for key %d. got %s, expected %s", expect.key, val.valueBytes, expect.val)
			return StopScanning, nil
		}
		idx--
		if idx >= 0 {
			return KeepScanning, nil
		} else {
			return StopScanning, nil
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}
