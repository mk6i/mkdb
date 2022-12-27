package storage

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestEncodeDecodeInternalNode(t *testing.T) {

	pg := &internalNode{
		node: node{
			fileOffset: 10,
			offsets:    []uint16{2, 1, 0, 3},
			freeSize:   4017,
		},
		cells: []*internalNodeCell{
			{
				key:        123,
				fileOffset: 3,
			},
			{
				key:        12,
				fileOffset: 8,
			},
			{
				key:        1,
				fileOffset: 6,
			},
			{
				key:        1234,
				fileOffset: 2,
			},
		},
		rightOffset: 1,
	}

	buf, err := pg.encode()
	if err != nil {
		t.Fatal(err)
	}

	if buf.Len() != pageSize {
		t.Fatalf("page size is not %d bytes, got %d\n", pageSize, buf.Cap())
	}

	actual := &internalNode{}
	if err = actual.decode(buf); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(pg, actual) {
		t.Errorf("Structs are not the same: %v\n%v", pg, actual)
	}
}

func TestEncodeDecodeLeafNode(t *testing.T) {

	pg := &leafNode{
		node: node{
			fileOffset: 10,
			freeSize:   3953,
			offsets:    []uint16{2, 1, 0, 3},
		},
		cells: []*leafNodeCell{
			{
				key:        1,
				valueSize:  uint32(len("lorem ipsum")),
				valueBytes: []byte("lorem ipsum"),
			},
			{
				key:        2,
				valueSize:  uint32(len("dolor sit amet")),
				valueBytes: []byte("dolor sit amet"),
			},
			{
				key:        3,
				valueSize:  uint32(len("consectetur adipiscing elit")),
				valueBytes: []byte("consectetur adipiscing elit"),
			},
			{
				key:        4,
				valueSize:  uint32(len("sed do eiusmod")),
				valueBytes: []byte("sed do eiusmod"),
			},
		},
	}

	buf, err := pg.encode()
	if err != nil {
		t.Fatal(err)
	}

	if len(buf.Bytes()) != pageSize {
		t.Fatalf("page size is not %d bytes, got %d\n", pageSize, buf.Cap())
	}

	actual := &leafNode{}
	if err = actual.decode(buf); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(pg, actual) {
		t.Errorf("Structs are not the same: %v\n%v", pg, actual)
	}
}

func TestMemoryStore(t *testing.T) {

	pages := []*leafNode{{}, {}, {}}

	m := &memoryStore{}

	for _, p := range pages {
		if err := m.append(p); err != nil {
			t.Fatal(err)
		}
	}

	for idx, p := range pages {
		fp, err := m.fetch(uint64(idx))
		if err != nil {
			t.Errorf("unable to fetch page at expectedOffset %d", idx)
		}
		if fp != p {
			t.Errorf("page at expectedOffset %d is not the same as the one inserted", idx)
		}
	}
}

func TestFileStore(t *testing.T) {

	fs1 := newFileStore("/tmp/page_file")
	fs1.nextFreeOffset = pageSize

	defer os.Remove(fs1.path)

	err := fs1.save()
	if err != nil {
		t.Errorf("unable to save file store: %s", err.Error())
	}

	fs2 := newFileStore("/tmp/page_file")

	err = fs2.open()
	if err != nil {
		t.Errorf("unable to open file store: %s", err.Error())
	}

	if fs1.nextFreeOffset != fs2.nextFreeOffset {
		t.Errorf("file store branch factors do not match")
	}

	root := &leafNode{}
	if err := fs2.append(root); err != nil {
		t.Errorf("error appending root: %s", err.Error())
	}
	fs2.setRoot(root)

	root2, err := fs2.getRoot()
	if err != nil {
		t.Errorf("unable to fetch root: %s", err.Error())
	}

	if reflect.TypeOf(root) != reflect.TypeOf(root2) {
		t.Errorf("root cell types do not match")
	}
}

func TestFindCellByKey(t *testing.T) {
	pages := []interface{}{
		&internalNode{
			node: node{
				offsets: []uint16{
					0, 1, 2, 3, 4,
				},
			},
			cells: []*internalNodeCell{
				{key: 1},
				{key: 3},
				{key: 5},
				{key: 7},
				{key: 9},
			},
		},
		&leafNode{
			node: node{
				offsets: []uint16{
					0, 1, 2, 3, 4,
				},
			},
			cells: []*leafNodeCell{
				{key: 1},
				{key: 3},
				{key: 5},
				{key: 7},
				{key: 9},
			},
		},
	}

	for _, pg := range pages {
		findCellByKeyTestCase(t, pg)
	}
}

func findCellByKeyTestCase(t *testing.T, pg interface{}) {

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

		var expectedOffset int
		var expectedFound bool

		switch pg := pg.(type) {
		case *internalNode:
			expectedOffset, expectedFound = pg.findCellOffsetByKey(v.key)
		case *leafNode:
			expectedOffset, expectedFound = pg.findCellOffsetByKey(v.key)
		}

		if expectedOffset != v.expectedOffset || expectedFound != v.expectedFound {
			t.Errorf("[key]: %d [page]: %v [expectedOffset]: %d [actualOffset]: %d [expectedFound]: %t [actualFound]: %t",
				v.key, pg, v.expectedOffset, expectedOffset, v.expectedFound, expectedFound)
		}
	}
}

func TestIsFullLeafNodeExpectFull(t *testing.T) {
	pg := &leafNode{}

	for i := 0; i < maxLeafNodeCells; i++ {
		if err := pg.appendCell(uint32(i), []byte("hello")); err != nil {
			t.Fatal(err)
		}
	}

	if !pg.isFull() {
		t.Errorf("leaf node is supposed to be full but is not. max leaf node cells: %d", maxLeafNodeCells)
	}
}

func TestIsFullLeafNodeExpectNotFull(t *testing.T) {
	pg := &leafNode{}

	for i := 0; i < maxLeafNodeCells-1; i++ {
		if err := pg.appendCell(uint32(i), []byte("hello")); err != nil {
			t.Fatal(err)
		}
	}

	if pg.isFull() {
		t.Errorf("leaf node is not supposed to be full, but it is. max leaf node cells: %d", maxLeafNodeCells)
	}
}

func TestIsFullInternalNodeExpectFull(t *testing.T) {
	pg := &internalNode{}

	for i := 0; i < maxInternalNodeCells; i++ {
		if err := pg.appendCell(uint32(i), 1); err != nil {
			t.Fatal(err)
		}
	}

	if !pg.isFull() {
		t.Errorf("internal node is supposed to be full but is not. branch factor: %d", maxInternalNodeCells)
	}
}

func TestIsFullInternalNodeExpectNotFull(t *testing.T) {
	pg := &internalNode{}

	for i := 0; i < maxInternalNodeCells-1; i++ {
		if err := pg.appendCell(uint32(i), 1); err != nil {
			t.Fatal(err)
		}
	}

	if pg.isFull() {
		t.Errorf("internal node is not supposed to be full, but it is. branch factor: %d", maxInternalNodeCells)
	}
}

func TestSplitLeafNode(t *testing.T) {
	pg := &leafNode{}

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

	newPg := &leafNode{}
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
		&leafNodeCell{
			key:        0,
			valueSize:  uint32(len([]byte("hello 0"))),
			valueBytes: []byte("hello 0"),
		},
		&leafNodeCell{
			key:        1,
			valueSize:  uint32(len([]byte("hello 1"))),
			valueBytes: []byte("hello 1"),
		},
	}

	for i := 0; i < len(expected); i++ {
		actual := pg.cells[pg.offsets[i]]
		if !reflect.DeepEqual(actual, expected[i]) {
			t.Errorf("key value cell does not match. expected: %+v actual: %+v", expected[i], actual)
		}
	}

	expected = []interface{}{
		&leafNodeCell{
			key:        2,
			valueSize:  uint32(len([]byte("hello 2"))),
			valueBytes: []byte("hello 2"),
		},
		&leafNodeCell{
			key:        3,
			valueSize:  uint32(len([]byte("hello 3"))),
			valueBytes: []byte("hello 3"),
		},
	}

	for i := 0; i < len(expected); i++ {
		actual := newPg.cells[pg.offsets[i]]
		if !reflect.DeepEqual(actual, expected[i]) {
			t.Errorf("key value cell does not match. expected: %+v actual: %+v", expected[i], actual)
		}
	}
}

func TestLeafNodeSizeLimit(t *testing.T) {

	tbl := []struct {
		name      string
		fn        func() error
		expectErr error
	}{
		{
			name: "insert cell at size limit",
			fn: func() error {
				pg := &leafNode{}
				return pg.insertCell(0, 0, make([]byte, maxValueSize))
			},
			expectErr: nil,
		},
		{
			name: "update cell at size limit",
			fn: func() error {
				pg := &leafNode{}
				err := pg.insertCell(0, 0, []byte("test"))
				if err != nil {
					return err
				}
				return pg.updateCell(0, make([]byte, maxValueSize))
			},
			expectErr: nil,
		},
		{
			name: "insert cell over size limit",
			fn: func() error {
				pg := &leafNode{}
				return pg.insertCell(0, 0, make([]byte, maxValueSize+1))
			},
			expectErr: ErrRowTooLarge,
		},
		{
			name: "update cell over size limit",
			fn: func() error {
				pg := &leafNode{}
				err := pg.insertCell(0, 0, []byte("test"))
				if err != nil {
					return err
				}
				return pg.updateCell(0, make([]byte, maxValueSize+1))
			},
			expectErr: ErrRowTooLarge,
		},
	}

	for _, tt := range tbl {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if !errors.Is(err, tt.expectErr) {
				t.Errorf("expected error `%v`, got %v", tt.expectErr, err)
			}
		})
	}

}
