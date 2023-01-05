package storage

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

type mockFile struct {
	*bytes.Buffer
}

func (m *mockFile) Close() error {
	return nil
}

func (m *mockFile) Sync() error {
	return nil
}

func TestWal(t *testing.T) {
	w := &wal{
		reader: &mockFile{
			bytes.NewBuffer(make([]byte, 100)),
		},
	}

	expBatch := WALBatch{
		{
			WALOp:  OP_UPDATE,
			pageID: 1234,
			cellID: 5678,
			val:    []byte{1, 2, 3, 4},
		},
		{
			WALOp:  OP_INSERT,
			pageID: 9101112,
			cellID: 13141516,
			val:    []byte{1, 2, 3, 4},
		},
		{
			WALOp:  OP_DELETE,
			pageID: 17181920,
			cellID: 21222324,
		},
	}

	if err := w.flush(expBatch); err != nil {
		t.Fatalf("failed flushing WAL batch: %s", err.Error())
	}

	actlBatch, err := w.read()
	if err != nil {
		t.Fatalf("failed reading WAL batch: %s", err.Error())
	}

	if reflect.DeepEqual(expBatch, actlBatch) {
		t.Fatal("actual WAL batch does not match expected WAL batch")
	}
}

func TestWalReplay(t *testing.T) {

	file, err := ioutil.TempFile("", "fs")
	if err != nil {
		t.Fatalf("error creating tmp file: %s", err.Error())
	}
	defer os.Remove(file.Name())

	fs, err := newFileStore(file.Name())
	if err != nil {
		t.Fatalf("error creating file store: %s", err.Error())
	}

	var pg btreeNode
	pg = &leafNode{}
	pg.(*leafNode).appendCell(0, []byte{0, 0, 0, 0})
	pg.(*leafNode).appendCell(1, []byte{5, 6, 7, 8})

	if err := fs.append(pg); err != nil {
		t.Fatalf("failed to append page: %s", err.Error())
	}

	batch := WALBatch{
		{
			WALOp:  OP_UPDATE,
			pageID: 0,
			cellID: 0,
			val:    []byte{1, 2, 3, 4},
		},
		{
			WALOp:  OP_DELETE,
			pageID: 0,
			cellID: 1,
		},
		{
			WALOp:  OP_INSERT,
			pageID: 0,
			cellID: 2,
			val:    []byte{5, 6, 7, 8},
		},
	}

	if err := batch.replay(fs); err != nil {
		t.Fatalf("failed to replay WAL batch: %s", err.Error())
	}

	fs.close()

	// new file store in order to start with fresh cache
	fs, err = newFileStore(file.Name())
	if err != nil {
		t.Fatalf("error creating file store: %s", err.Error())
	}

	pg, err = fs.fetch(0)
	if err != nil {
		t.Fatalf("failed to fetch page: %s", err)
	}

	// verify UPDATE
	actual := pg.(*leafNode).cells[0].valueBytes
	expected := []byte{1, 2, 3, 4}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("cell payload is not the same. expected: %v actual: %v", expected, actual)
	}

	// verify DELETE
	if !pg.(*leafNode).cells[1].deleted {
		t.Fatal("expected cell 1 to be deleted, but it is not")
	}

	// verify INSERT
	actual = pg.(*leafNode).cells[2].valueBytes
	expected = []byte{5, 6, 7, 8}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("cell payload is not the same. expected: %v actual: %v", expected, actual)
	}
}
