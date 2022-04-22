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

func TestBinarySearch(t *testing.T) {

	tbl := []struct {
		needle uint16
		elems  []uint16
		offset int
		found  bool
	}{
		{
			needle: 5,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 2,
			found:  true,
		},
		{
			needle: 3,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 1,
			found:  true,
		},
		{
			needle: 7,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 3,
			found:  true,
		},
		{
			needle: 1,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 0,
			found:  true,
		},
		{
			needle: 9,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 4,
			found:  true,
		},
		{
			needle: 0,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 0,
			found:  false,
		},
		{
			needle: 2,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 1,
			found:  false,
		},
		{
			needle: 4,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 2,
			found:  false,
		},
		{
			needle: 6,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 3,
			found:  false,
		},
		{
			needle: 8,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 4,
			found:  false,
		},
		{
			needle: 10,
			elems:  []uint16{1, 3, 5, 7, 9},
			offset: 5,
			found:  false,
		},
	}

	for _, v := range tbl {
		offset, found := binarySearch(v.needle, v.elems)
		if offset != v.offset || found != v.found {
			t.Errorf("[needle]: %d [elems]: %v [expected offset]: %d [actual offset]: %d [expected found]: %t [actual found]: %t",
				v.needle, v.elems, v.offset, offset, v.found, found)
		}
	}
}
