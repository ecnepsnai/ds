package ds

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
)

func gobEncode(i interface{}) ([]byte, error) {
	if i == nil {
		return nil, fmt.Errorf("gobEncode: nil value provided")
	}
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(i)
	if err != nil {
		return nil, fmt.Errorf("gobEncode: %s", err.Error())
	}
	if buf.Len() == 0 {
		return nil, fmt.Errorf("gobEncode: no bytes returned")
	}
	return buf.Bytes(), nil
}

func gobDecodePrimaryKeyList(b []byte) ([][]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("gobDecodePrimaryKeyList: nil data provided")
	}

	var w [][]byte

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("gobDecodePrimaryKeyList: %s", err.Error())
	}
	return w, nil
}

func (table *Table) gobDecodeValue(b []byte) (reflect.Value, error) {
	if len(b) == 0 {
		return reflect.Value{}, fmt.Errorf("gobDecodeValue: nil data provided")
	}

	value := reflect.New(table.typeOf).Elem()
	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.DecodeValue(value); err != nil {
		return value, fmt.Errorf("gobDecodeValue: %s", err.Error())
	}
	return value, nil
}

func gobDecodeConfig(b []byte) (*Config, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("gobDecodeConfig: nil data provided")
	}

	var w = Config{}

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("gobDecodeConfig: %s", err.Error())
	}
	return &w, nil
}

func gobDecodeOptions(b []byte) (*Options, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("gobDecodeOptions: nil data provided")
	}

	var w = Options{}

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("gobDecodeOptions: %s", err.Error())
	}
	return &w, nil
}
