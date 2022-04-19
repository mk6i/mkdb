package btree

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBasic(t *testing.T) {

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
