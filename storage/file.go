package storage

import (
	"os"
	"path/filepath"
	"strings"
)

const dataPath = "data"

func makeDBDir(db string) error {
	err := os.MkdirAll(filepath.Join(dataPath, strings.ToLower(db)), 0755)
	if !os.IsExist(err) {
		return err
	}
	return nil
}

func listDBs() ([]string, error) {
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	files, err := f.Readdir(0)
	if err != nil {
		return nil, err
	}

	var dbs []string
	for _, v := range files {
		if v.IsDir() {
			dbs = append(dbs, v.Name())
		}
	}
	return dbs, nil
}

func dbFilePath(db string) (string, bool, error) {
	if db == "" {
		return "", false, ErrDBNotSelected
	}

	path := filepath.Join(dataPath, strings.ToLower(db), "tbl")

	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return path, false, err
	}

	return path, !os.IsNotExist(err), nil
}

func walFilePath(db string) (string, bool, error) {
	if db == "" {
		return "", false, ErrDBNotSelected
	}

	path := filepath.Join(dataPath, strings.ToLower(db), "wal")

	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return path, false, err
	}

	return path, !os.IsNotExist(err), nil
}

func MakeDataDir() error {
	if err := os.MkdirAll(dataPath, 0755); err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func ClearDataDir() error {
	return os.RemoveAll(dataPath)
}
