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
		forceSync: true,
	}

	expBatch := WALBatch{
		{
			WALOp:  OpUpdate,
			pageID: 1234,
			cellID: 5678,
			val:    []byte{1, 2, 3, 4},
		},
		{
			WALOp:  OpInsert,
			pageID: 9101112,
			cellID: 13141516,
			val:    []byte{1, 2, 3, 4},
		},
		{
			WALOp:  OpDelete,
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

	fs, err := newFileStore(file.Name(), false)
	if err != nil {
		t.Fatalf("error creating file store: %s", err.Error())
	}
	fs.nextFreeOffset = pageSize

	if err := fs.save(); err != nil {
		t.Fatalf("err: %s", err)
	}

	pg := &btreeNode{isLeaf: true}
	if err := pg.appendLeafCell(0, []byte{0, 0, 0, 0}); err != nil {
		return
	}
	if err := pg.appendLeafCell(1, []byte{5, 6, 7, 8}); err != nil {
		return
	}

	if err := fs.append(pg); err != nil {
		t.Fatalf("failed to append page: %s", err.Error())
	}

	batch := WALBatch{
		{
			WALOp:  OpUpdate,
			pageID: pg.getFileOffset(),
			cellID: 0,
			val:    []byte{1, 2, 3, 4},
			LSN:    1,
		},
		{
			WALOp:  OpDelete,
			pageID: pg.getFileOffset(),
			cellID: 1,
			LSN:    2,
		},
		{
			WALOp:  OpInsert,
			pageID: pg.getFileOffset(),
			cellID: 2,
			val:    []byte{5, 6, 7, 8},
			LSN:    3,
		},
	}

	if err := batch.replay(fs); err != nil {
		t.Fatalf("failed to replay WAL batch: %s", err.Error())
	}

	if err := fs.close(); err != nil {
		t.Fatal(err)
	}

	// new file store in order to start with fresh cache
	fs, err = newFileStore(file.Name(), false)
	if err != nil {
		t.Fatalf("error creating file store: %s", err.Error())
	}

	pg, err = fs.fetch(pg.getFileOffset())
	if err != nil {
		t.Fatalf("failed to fetch page: %s", err)
	}

	// verify UPDATE
	actual := pg.leafCells[0].valueBytes
	expected := []byte{1, 2, 3, 4}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("cell payload is not the same. expected: %v actual: %v", expected, actual)
	}

	// verify DELETE
	if !pg.leafCells[1].deleted {
		t.Fatal("expected cell 1 to be deleted, but it is not")
	}

	// verify INSERT
	actual = pg.leafCells[2].valueBytes
	expected = []byte{5, 6, 7, 8}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("cell payload is not the same. expected: %v actual: %v", expected, actual)
	}
}
