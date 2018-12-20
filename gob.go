package ds

import (
	"bytes"
	"encoding/gob"
	"reflect"
)

func gobEncode(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(i)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecodePrimaryKeyList(b []byte) ([][]byte, error) {
	var w [][]byte

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, err
	}
	return w, nil
}

func (table *Table) gobDecodeValue(b []byte) (reflect.Value, error) {
	value := reflect.New(table.typeOf).Elem()
	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.DecodeValue(value); err != nil {
		return value, err
	}
	return value, nil
}

func gobDecodeConfig(b []byte) (*Config, error) {
	var w = Config{}

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, err
	}
	return &w, nil
}
