package main

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestReadLine(t *testing.T) {

	in := bytes.NewBuffer(nil)
	in.WriteString("INSERT INTO season (number, year) VALUES\n\r" +
		"(1, 2008),\n\r" +
		"(2, 2009),\n\r" +
		"(3, 2010),\n\r" +
		"(4, 2011),\n\r" +
		"(5, 2012);\n\r" +
		"INSERT INTO famous_lines (name, quote, season) VALUES\n\r" +
		"('Walter', 'Chemistry is, well technically, chemistry is the study of matter. But I prefer to see it as the " +
		"study of change.', 1),\n\r" +
		"('Walter', 'Oh, yes. Now we just need to figure out a delivery device, and then no more Tuco', 2)\n\r" +
		"('Walter', 'How was I supposed to know you were chauffeuring Tuco to my doorstep?', 2),\n\r" +
		"('Skyler', 'We have discussed everything we need to discuss... I thought I made myself very clear.', 3);\n\r" +
		"CREATE DATABASE testdb; USE testdb;\n\r" +
		"CREATE DATABASE testdb; \n\r")

	term := NewTerminal(in, "")

	expect := [][]string{
		{
			"INSERT INTO season (number, year) VALUES" +
				" (1, 2008)," +
				" (2, 2009)," +
				" (3, 2010)," +
				" (4, 2011)," +
				" (5, 2012);",
		},
		{
			"INSERT INTO famous_lines (name, quote, season) VALUES" +
				" ('Walter', 'Chemistry is, well technically, chemistry is the study of matter. But I prefer to see it as the " +
				"study of change.', 1)," +
				" ('Walter', 'Oh, yes. Now we just need to figure out a delivery device, and then no more Tuco', 2)" +
				" ('Walter', 'How was I supposed to know you were chauffeuring Tuco to my doorstep?', 2)," +
				" ('Skyler', 'We have discussed everything we need to discuss... I thought I made myself very clear.', 3);",
		},
		{
			"CREATE DATABASE testdb;",
			"USE testdb;",
		},
		{
			"CREATE DATABASE testdb;",
		},
	}

	for _, str := range expect {
		line, err := term.ReadLine()
		if err != nil && err != io.EOF {
			t.Fatalf("error reading line: %s", err.Error())
		}

		if !reflect.DeepEqual(str, line) {
			t.Errorf("unxpected line read. expected: %s actual: %s", str, line)
		}
	}
}
