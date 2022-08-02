package btree

import (
	"reflect"
	"testing"
)

func TestTableSchemaEncodeDecode(t *testing.T) {

	ts1 := &TableSchema{
		Fields: []FieldDef{
			{
				dtype: TYPE_INT,
				name:  "id",
			},
			{
				dtype: TYPE_VARCHAR,
				len:   255,
				name:  "first_name",
			},
			{
				dtype: TYPE_VARCHAR,
				len:   255,
				name:  "last_name",
			},
		},
	}

	encoded, err := ts1.Encode()
	if err != nil {
		t.Errorf("error encoding table schema: %s", err.Error())
	}

	ts2 := &TableSchema{}

	if err := ts2.Decode(encoded); err != nil {
		t.Errorf("error decoding table schema: %s", err.Error())
	}

	if !reflect.DeepEqual(ts1, ts2) {
		t.Error("encoded and decoded structs are not the same")
	}
}
