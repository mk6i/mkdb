package storage

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestRelationEncodeDecode(t *testing.T) {

	rel1 := &Relation{
		Fields: []FieldDef{
			{
				DataType: TypeInt,
				Name:     "id",
			},
			{
				DataType: TypeVarchar,
				Len:      255,
				Name:     "first_name",
			},
			{
				DataType: TypeVarchar,
				Len:      255,
				Name:     "last_name",
			},
			{
				DataType: TypeBoolean,
				Name:     "bool_val",
			},
			{
				DataType: TypeBigInt,
				Name:     "salary",
			},
			{
				DataType: TypeInt,
				Name:     "age",
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
				DataType: TypeInt,
				Name:     "id",
			},
			{
				DataType: TypeVarchar,
				Len:      255,
				Name:     "first_name",
			},
			{
				DataType: TypeVarchar,
				Len:      255,
				Name:     "last_name",
			},
			{
				DataType: TypeBoolean,
				Name:     "bool_val",
			},
			{
				DataType: TypeBigInt,
				Name:     "salary",
			},
			{
				DataType: TypeInt,
				Name:     "age",
			},
		},
	}

	tup1 := &Tuple{
		Vals: map[string]interface{}{
			"id":         int64(1234),
			"first_name": "John",
			"last_name":  "Doe",
			"bool_val":   true,
			"salary":     int64(33000000000),
			"age":        int64(35),
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

func TestTupleEncodeDecodeErrIntRange(t *testing.T) {

	tupOverflow := &Tuple{
		Vals: map[string]interface{}{
			"age": int64(math.MaxInt32 + 1),
		},
		Relation: &Relation{
			Fields: []FieldDef{
				{
					DataType: TypeInt,
					Name:     "age",
				},
			},
		},
	}

	_, err := tupOverflow.Encode()
	if !errors.Is(err, ErrIntOutOfRange) {
		t.Errorf("expected error `%v`, got `%v`", ErrIntOutOfRange, err)
	}

	tupUnderflow := &Tuple{
		Vals: map[string]interface{}{
			"age": int64(math.MinInt32 - 1),
		},
		Relation: &Relation{
			Fields: []FieldDef{
				{
					DataType: TypeInt,
					Name:     "age",
				},
			},
		},
	}

	_, err = tupUnderflow.Encode()
	if !errors.Is(err, ErrIntOutOfRange) {
		t.Errorf("expected error `%v`, got `%v`", ErrIntOutOfRange, err)
	}
}

func TestFieldsLookupColIdx(t *testing.T) {

	fields := Fields{
		&Field{
			TableID: "t1",
			Column:  "id",
		},
		&Field{
			TableID: "t2",
			Column:  "id",
		},
		&Field{
			TableID: "t2",
			Column:  "name",
		},
	}

	cases := []struct {
		givenColumn string
		expectedIdx int
		expectedErr error
	}{
		{
			givenColumn: "name",
			expectedIdx: 2,
			expectedErr: nil,
		},
		{
			givenColumn: "id",
			expectedIdx: -1,
			expectedErr: ErrFieldAmbiguous,
		},
		{
			givenColumn: "non-existent-field",
			expectedIdx: -1,
			expectedErr: ErrFieldNotFound,
		},
	}

	for _, c := range cases {
		idx, err := fields.LookupFieldIdx(c.givenColumn)
		if c.expectedErr != nil {
			if !errors.Is(err, c.expectedErr) {
				t.Errorf("expected error %v, got %v", c.expectedErr, err)
			}
		} else if err != nil {
			t.Errorf("failed to look up column index: %s", err.Error())
		}
		if c.expectedIdx != idx {
			t.Errorf("expected idx %d, got %d", c.expectedIdx, idx)
		}
	}
}

func TestFieldsLookupColIdxByID(t *testing.T) {

	fields := Fields{
		&Field{
			TableID: "t1",
			Column:  "id",
		},
		&Field{
			TableID: "t2",
			Column:  "id",
		},
		&Field{
			TableID: "t2",
			Column:  "name",
		},
	}

	cases := []struct {
		givenTableID   string
		givenFieldName string
		expectedIdx    int
		expectedErr    error
	}{
		{
			givenTableID:   "t2",
			givenFieldName: "name",
			expectedIdx:    2,
			expectedErr:    nil,
		},
		{
			givenTableID:   "t1",
			givenFieldName: "id",
			expectedIdx:    0,
			expectedErr:    nil,
		},
		{
			givenTableID:   "t2",
			givenFieldName: "id",
			expectedIdx:    1,
			expectedErr:    nil,
		},
		{
			givenTableID:   "t1",
			givenFieldName: "name",
			expectedIdx:    -1,
			expectedErr:    ErrFieldNotFound,
		},
	}

	for _, c := range cases {
		idx, err := fields.LookupColIdxByID(c.givenTableID, c.givenFieldName)
		if c.expectedErr != nil {
			if !errors.Is(err, c.expectedErr) {
				t.Errorf("expected error %v, got %v", c.expectedErr, err)
			}
		} else if err != nil {
			t.Errorf("failed to look up column index: %s", err.Error())
		}
		if c.expectedIdx != idx {
			t.Errorf("expected idx %d, got %d", c.expectedIdx, idx)
		}
	}
}
