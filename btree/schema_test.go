package btree

import (
	"reflect"
	"testing"
)

func TestRelationEncodeDecode(t *testing.T) {

	rel1 := &Relation{
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

	encoded, err := rel1.Encode()
	if err != nil {
		t.Errorf("error encoding tuple: %s", err.Error())
	}

	rel2 := &Relation{}

	if err := rel2.Decode(encoded); err != nil {
		t.Errorf("error decoding tuple: %s", err.Error())
	}

	if !reflect.DeepEqual(rel1, rel2) {
		t.Error("encoded and decoded tuple structs are not the same")
	}
}

func TestTupleEncodeDecode(t *testing.T) {

	rel := &Relation{
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

	tup1 := &Tuple{
		Vals: map[string]interface{}{
			"id":         int32(1234),
			"first_name": "John",
			"last_name":  "Doe",
		},
		Relation: rel,
	}

	encoded, err := tup1.Encode()
	if err != nil {
		t.Errorf("error encoding tuple: %s", err.Error())
	}

	tup2 := &Tuple{
		Vals:     map[string]interface{}{},
		Relation: rel,
	}

	if err := tup2.Decode(encoded); err != nil {
		t.Errorf("error decoding tuple: %s", err.Error())
	}

	if !reflect.DeepEqual(tup1, tup2) {
		t.Error("encoded and decoded tuple structs are not the same")
	}
}
