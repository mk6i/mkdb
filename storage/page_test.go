package btree

import (
	"bytes"
	"reflect"
	"testing"
)

func TestEncodeDecode(t *testing.T) {

	page := &page{
		cellType:  KeyCell,
		cellCount: 4,
		offsets:   []uint16{2, 1, 0, 4},
		freeSize:  4049,
		cells: []interface{}{
			keyCell{
				key:    123,
				pageID: 3,
			},
			keyCell{
				key:    12,
				pageID: 8,
			},
			keyCell{
				key:    1,
				pageID: 6,
			},
			keyCell{
				key:    1234,
				pageID: 2,
			},
		},
	}

	var rp pageBuffer
	rp.buf = bytes.NewBuffer(make([]byte, 0))
	rp.encode(page)

	if rp.buf.Len() != 4096 {
		t.Fatalf("page size is not 4096 bytes, got %d\n", rp.buf.Cap())
	}

	actual := rp.decode()

	if !reflect.DeepEqual(page, actual) {
		t.Errorf("Structs are not the same: %v\n%v", page, actual)
	}
}

func TestMemoryStore(t *testing.T) {

	pages := []*page{
		&page{},
		&page{},
		&page{},
	}

	m := &memoryStore{}

	for _, p := range pages {
		m.append(p)
	}

	for idx, p := range pages {
		fp, err := m.fetch(uint16(idx))
		if err != nil {
			t.Errorf("unable to fetch page at expectedOffset %d", idx)
		}
		if fp != p {
			t.Errorf("page at expectedOffset %d is not the same as the one inserted", idx)
		}
	}
}

func TestFindCellByKey(t *testing.T) {
	pages := []*page{
		{
			cellType: KeyCell,
			cells: []interface{}{
				keyCell{key: 1},
				keyCell{key: 3},
				keyCell{key: 5},
				keyCell{key: 7},
				keyCell{key: 9},
			},
		},
		{
			cellType: KeyValueCell,
			cells: []interface{}{
				keyValueCell{key: 1},
				keyValueCell{key: 3},
				keyValueCell{key: 5},
				keyValueCell{key: 7},
				keyValueCell{key: 9},
			},
		},
	}

	for _, pg := range pages {
		findCellByKeyTestCase(t, pg)
	}
}

func findCellByKeyTestCase(t *testing.T, pg *page) {

	tbl := []struct {
		key            uint32
		expectedOffset int
		expectedFound  bool
	}{
		{
			key:            5,
			expectedOffset: 2,
			expectedFound:  true,
		},
		{
			key:            3,
			expectedOffset: 1,
			expectedFound:  true,
		},
		{
			key:            7,
			expectedOffset: 3,
			expectedFound:  true,
		},
		{
			key:            1,
			expectedOffset: 0,
			expectedFound:  true,
		},
		{
			key:            9,
			expectedOffset: 4,
			expectedFound:  true,
		},
		{
			key:            0,
			expectedOffset: 0,
			expectedFound:  false,
		},
		{
			key:            2,
			expectedOffset: 1,
			expectedFound:  false,
		},
		{
			key:            4,
			expectedOffset: 2,
			expectedFound:  false,
		},
		{
			key:            6,
			expectedOffset: 3,
			expectedFound:  false,
		},
		{
			key:            8,
			expectedOffset: 4,
			expectedFound:  false,
		},
		{
			key:            10,
			expectedOffset: 5,
			expectedFound:  false,
		},
	}

	for _, v := range tbl {
		expectedOffset, expectedFound := pg.findCellByKey(v.key)
		if expectedOffset != v.expectedOffset || expectedFound != v.expectedFound {
			t.Errorf("[key]: %d [page]: %v [expectedOffset]: %d [actualOffset]: %d [expectedFound]: %t [actualFound]: %t",
				v.key, pg, v.expectedOffset, expectedOffset, v.expectedFound, expectedFound)
		}
	}
}
