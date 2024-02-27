package engine

import (
	"testing"

	"github.com/mk6i/mkdb/storage"
)

func TestShowDatabase(t *testing.T) {
	defer storage.ClearDataDir()
	s := Session{}
	cases := []struct {
		q    string
		gold string
	}{
		{"SHOW DATABASE", "0 Result"},
		{"SHOW DATABASES", "0 Result"},
	}
	for _, c := range cases {
		if err := s.ExecQuery(c.q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", c.q, err.Error())
		}
	}
	q := `CREATE DATABASE testshow`
	err := s.ExecQuery(q)
	if err != nil {
		t.Error(q)
	}
	for _, c := range cases {
		if err := s.ExecQuery(c.q); err != nil {
			t.Errorf("error running query:\n %s\nError: %s", c.q, err.Error())
		}
	}

}
