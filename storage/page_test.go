package btree

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestEncodeDecodeKeyCell(t *testing.T) {

	page := &page{
		pageID:   10,
		cellType: KeyCell,
		offsets:  []uint16{2, 1, 0, 3},
		freeSize: 4031,
		cells: []interface{}{
			&keyCell{
				key:    123,
				pageID: 3,
			},
			&keyCell{
				key:    12,
				pageID: 8,
			},
			&keyCell{
				key:    1,
				pageID: 6,
			},
			&keyCell{
				key:    1234,
				pageID: 2,
			},
		},
		rightOffset: 1,
		hasLSib:     true,
		hasRSib:     true,
		lSibPageID:  2,
		rSibPageID:  3,
	}

	var rp pageBuffer
	rp.buf = bytes.NewBuffer(make([]byte, 0))
	if err := rp.encode(page); err != nil {
		t.Fatal(err)
	}

	if rp.buf.Len() != 4096 {
		t.Fatalf("page size is not 4096 bytes, got %d\n", rp.buf.Cap())
	}

	actual := rp.decode()

	if !reflect.DeepEqual(page, actual) {
		t.Errorf("Structs are not the same: %v\n%v", page, actual)
	}
}

func TestEncodeDecodeKeyValueCell(t *testing.T) {

	page := &page{
		pageID:   10,
		cellType: KeyValueCell,
		offsets:  []uint16{2, 1, 0, 3},
		freeSize: 3965,
		cells: []interface{}{
			&keyValueCell{
				key:        1,
				valueSize:  uint32(len("lorem ipsum")),
				valueBytes: []byte("lorem ipsum"),
			},
			&keyValueCell{
				key:        2,
				valueSize:  uint32(len("dolor sit amet")),
				valueBytes: []byte("dolor sit amet"),
			},
			&keyValueCell{
				key:        3,
				valueSize:  uint32(len("consectetur adipiscing elit")),
				valueBytes: []byte("consectetur adipiscing elit"),
			},
			&keyValueCell{
				key:        4,
				valueSize:  uint32(len("sed do eiusmod")),
				valueBytes: []byte("sed do eiusmod"),
			},
		},
	}

	var rp pageBuffer
	rp.buf = bytes.NewBuffer(make([]byte, 0))
	if err := rp.encode(page); err != nil {
		t.Fatal(err)
	}

	if len(rp.buf.Bytes()) != 4096 {
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
		if err := m.append(p); err != nil {
			t.Fatal(err)
		}
	}

	for idx, p := range pages {
		fp, err := m.fetch(uint32(idx))
		if err != nil {
			t.Errorf("unable to fetch page at expectedOffset %d", idx)
		}
		if fp != p {
			t.Errorf("page at expectedOffset %d is not the same as the one inserted", idx)
		}
	}
}

func TestFileStore(t *testing.T) {

	fs1 := fileStore{
		path:           "/tmp/page_file",
		branchFactor:   4,
		nextFreeOffset: 4096,
	}

	defer os.Remove(fs1.path)

	err := fs1.save()
	if err != nil {
		t.Errorf("unable to save file store: %s", err.Error())
	}

	fs2 := fileStore{
		path: "/tmp/page_file",
	}

	err = fs2.open()
	if err != nil {
		t.Errorf("unable to open file store: %s", err.Error())
	}

	if fs1.branchFactor != fs2.branchFactor {
		t.Errorf("file store branch factors do not match")
	}

	if fs1.nextFreeOffset != fs2.nextFreeOffset {
		t.Errorf("file store branch factors do not match")
	}

	root := &page{
		cellType: KeyValueCell,
	}
	if err := fs2.append(root); err != nil {
		t.Errorf("error appending root: %s", err.Error())
	}
	fs2.setRoot(root)

	root2, err := fs2.getRoot()
	if err != nil {
		t.Errorf("unable to fetch root: %s", err.Error())
	}

	if root.cellType != root2.cellType {
		t.Errorf("root cell types do not match")
	}
}

func TestFindCellByKey(t *testing.T) {
	pages := []*page{
		{
			cellType: KeyCell,
			offsets: []uint16{
				0, 1, 2, 3, 4,
			},
			cells: []interface{}{
				&keyCell{key: 1},
				&keyCell{key: 3},
				&keyCell{key: 5},
				&keyCell{key: 7},
				&keyCell{key: 9},
			},
		},
		{
			cellType: KeyValueCell,
			offsets: []uint16{
				0, 1, 2, 3, 4,
			},
			cells: []interface{}{
				&keyValueCell{key: 1},
				&keyValueCell{key: 3},
				&keyValueCell{key: 5},
				&keyValueCell{key: 7},
				&keyValueCell{key: 9},
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
		expectedOffset, expectedFound := pg.findCellOffsetByKey(v.key)
		if expectedOffset != v.expectedOffset || expectedFound != v.expectedFound {
			t.Errorf("[key]: %d [page]: %v [expectedOffset]: %d [actualOffset]: %d [expectedFound]: %t [actualFound]: %t",
				v.key, pg, v.expectedOffset, expectedOffset, v.expectedFound, expectedFound)
		}
	}
}

func TestSplitLeafNode(t *testing.T) {

	branchingFactor := uint32(4)

	pg := &page{
		cellType: KeyValueCell,
	}

	if err := pg.appendCell(0, []byte("hello 0")); err != nil {
		t.Fatal(err)
	}
	if err := pg.appendCell(1, []byte("hello 1")); err != nil {
		t.Fatal(err)
	}
	if err := pg.appendCell(2, []byte("hello 2")); err != nil {
		t.Fatal(err)
	}
	if err := pg.appendCell(3, []byte("hello 3")); err != nil {
		t.Fatal(err)
	}

	if !pg.isFull(branchingFactor) {
		t.Errorf("page is supposed to be full but is not. branching factor: %d", branchingFactor)
	}

	newPg := &page{}
	parentKey, err := pg.split(newPg)
	if err != nil {
		t.Fatal(err)
	}

	if parentKey != 2 {
		t.Errorf("parent key is unexpected. actual: %d", parentKey)
	}
	if len(newPg.cells) != 2 {
		t.Errorf("new page is supposed to be half size but is not. size: %d", len(newPg.cells))
	}

	expected := []interface{}{
		&keyValueCell{
			key:        0,
			valueSize:  uint32(len([]byte("hello 0"))),
			valueBytes: []byte("hello 0"),
		},
		&keyValueCell{
			key:        1,
			valueSize:  uint32(len([]byte("hello 1"))),
			valueBytes: []byte("hello 1"),
		},
	}

	for i := 0; i < len(expected); i++ {
		actual := pg.cells[pg.offsets[i]]
		if !reflect.DeepEqual(actual.(*keyValueCell), expected[i].(*keyValueCell)) {
			t.Errorf("key value cell does not match. expected: %+v actual: %+v", expected[i], actual)
		}
	}

	expected = []interface{}{
		&keyValueCell{
			key:        2,
			valueSize:  uint32(len([]byte("hello 2"))),
			valueBytes: []byte("hello 2"),
		},
		&keyValueCell{
			key:        3,
			valueSize:  uint32(len([]byte("hello 3"))),
			valueBytes: []byte("hello 3"),
		},
	}

	for i := 0; i < len(expected); i++ {
		actual := newPg.cells[pg.offsets[i]]
		if !reflect.DeepEqual(actual.(*keyValueCell), expected[i].(*keyValueCell)) {
			t.Errorf("key value cell does not match. expected: %+v actual: %+v", expected[i], actual)
		}
	}
}
