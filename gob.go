package ds

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
)

func gobEncode(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(i)
	if err != nil {
		return nil, fmt.Errorf("gobEncode: %s", err.Error())
	}
	return buf.Bytes(), nil
}

func gobDecodePrimaryKeyList(b []byte) ([][]byte, error) {
	var w [][]byte

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("gobDecodePrimaryKeyList: %s", err.Error())
	}
	return w, nil
}

func (table *Table) gobDecodeValue(b []byte) (reflect.Value, error) {
	value := reflect.New(table.typeOf).Elem()
	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.DecodeValue(value); err != nil {
		return value, fmt.Errorf("gobDecodeValue: %s", err.Error())
	}
	return value, nil
}

func gobDecodeConfig(b []byte) (*Config, error) {
	var w = Config{}

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("gobDecodeConfig: %s", err.Error())
	}
	return &w, nil
}

func gobDecodeOptions(b []byte) (*Options, error) {
	var w = Options{}

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("gobDecodeOptions: %s", err.Error())
	}
	return &w, nil
}
