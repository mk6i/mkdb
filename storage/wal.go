package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type (
	WALOp    uint8
	WALBatch []*WALEntry
)

const (
	OP_INSERT WALOp = iota
	OP_UPDATE
	OP_DELETE
)

func InitStorage() error {
	if err := MakeDataDir(); err != nil {
		return fmt.Errorf("error creating data dir: %w", err)
	}

	dbs, err := listDBs()
	if err != nil {
		return err
	}

	for _, db := range dbs {
		path, exists, err := dbFilePath(db)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		wal, err := newWal(db)
		if err != nil {
			return err
		}

		defer wal.close()

		fs, err := newFileStore(path)
		if err != nil {
			return err
		}

		defer fs.close()

		batch, err := wal.read()
		if err != nil {
			return err
		}

		if err := batch.replay(fs); err != nil {
			return fmt.Errorf("WAL replay error: %w", err)
		}
	}

	return nil
}

type WALEntry struct {
	WALOp
	pageID uint64
	cellID uint32
	val    []byte
}

func (w *WALEntry) encode() (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, w.WALOp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, w.pageID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, w.cellID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(w.val))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, w.val); err != nil {
		return nil, err
	}

	return buf, nil
}

func (w *WALEntry) decode(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.LittleEndian, &w.WALOp); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &w.pageID); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &w.cellID); err != nil {
		return err
	}

	var newSize uint32
	if err := binary.Read(buf, binary.LittleEndian, &newSize); err != nil {
		return err
	}

	strBuf := make([]byte, newSize)
	if _, err := buf.Read(strBuf); err != nil {
		return err
	}
	w.val = strBuf

	return nil
}

func newWal(db string) (*wal, error) {
	path, _, err := walFilePath(db)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &wal{
		reader: file,
	}, nil
}

type readWriteSyncCloser interface {
	io.Reader
	io.Writer
	io.Closer
	Sync() error
}

type wal struct {
	reader readWriteSyncCloser
}

func (w *wal) close() error {
	return w.reader.Close()
}

func (w *wal) read() (WALBatch, error) {
	var ret WALBatch
	reader := bufio.NewReader(w.reader)
	tupleLenBuf := make([]byte, 4)

	for {
		if n, err := reader.Read(tupleLenBuf); err == io.EOF || n == 0 {
			break
		} else if err != nil {
			return ret, err
		} else if n != len(tupleLenBuf) {
			panic("bytes read differs from expected buffer length")
		}

		tupleLen := int(binary.LittleEndian.Uint32(tupleLenBuf))
		if tupleLen == 0 {
			break
		}

		tupleBuf := make([]byte, tupleLen)
		if n, err := reader.Read(tupleBuf); err != nil {
			return ret, err
		} else if n != tupleLen {
			panic("bytes read differs from expected buffer length")
		}

		w := &WALEntry{}
		if err := w.decode(bytes.NewBuffer(tupleBuf)); err != nil {
			return ret, err
		}
		ret = append(ret, w)
	}

	return ret, nil
}

func (w *wal) flush(batch WALBatch) error {
	tupleLenBuf := make([]byte, 4)

	for _, tuple := range batch {
		tupleBuf, err := tuple.encode()
		if err != nil {
			return err
		}

		tupleLen := len(tupleBuf.Bytes())
		binary.LittleEndian.PutUint32(tupleLenBuf, uint32(tupleLen))

		if n, err := w.reader.Write(tupleLenBuf); err != nil {
			return err
		} else if n != len(tupleLenBuf) {
			panic("bytes written differs from expected buffer length")
		}

		if n, err := w.reader.Write(tupleBuf.Bytes()); err != nil {
			return err
		} else if n != tupleLen {
			panic("bytes written differs from expected buffer length")
		}
	}

	if err := w.reader.Sync(); err != nil {
		return err
	}

	return nil
}

func (w WALBatch) replay(fs *fileStore) error {
	for _, row := range w {
		switch row.WALOp {
		case OP_INSERT:
			tablePg, err := fs.fetch(row.pageID)
			if err != nil {
				return err
			}
			bt := &BTree{store: fs}
			bt.setRoot(tablePg)
			err = bt.insertKey(row.cellID, row.val)
			if err != nil && !errors.Is(err, errKeyAlreadyExists) {
				return err
			}
		case OP_UPDATE:
			node, err := fs.fetch(row.pageID)
			if err != nil {
				return err
			}
			err = node.(*leafNode).updateCell(row.cellID, row.val)
			if err != nil {
				return nil
			}
			node.markDirty()
		case OP_DELETE:
			node, err := fs.fetch(row.pageID)
			if err != nil {
				return err
			}
			offset, found := node.(*leafNode).findCellOffsetByKey(row.cellID)
			if !found {
				return fmt.Errorf("unable to find cell for rowID %d", row.cellID)
			}
			node.(*leafNode).cells[offset].deleted = true
			node.markDirty()
		}
	}
	return fs.flushPages()
}
