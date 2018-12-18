package ds

import (
	"fmt"

	"github.com/boltdb/bolt"
)

// Get will get a single entry by its primary key. Returns (nil, nil) if nothing found.
func (table *Table) Get(primaryKey interface{}) (interface{}, error) {
	if primaryKey == nil {
		return nil, nil
	}

	primaryKeyBytes, err := gobEncode(primaryKey)
	if err != nil {
		table.log.Error("Error encoding primary key: %s", err.Error())
		return nil, err
	}

	return table.getPrimaryKey(primaryKeyBytes)
}

func (table *Table) getPrimaryKey(key []byte) (interface{}, error) {
	var data []byte
	err := table.data.View(func(tx *bolt.Tx) error {
		dataBucket := tx.Bucket([]byte("data"))
		data = dataBucket.Get(key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	value, err := table.gobDecodeValue(data)
	if err != nil {
		table.log.Error("Error decoding value: %s", err.Error())
		return nil, err
	}

	return value.Interface(), nil
}

// GetIndex will get multiple entries that contain the same value for the specified indexed field.
// Returns an empty array if nothing found.
func (table *Table) GetIndex(fieldName string, value interface{}) ([]interface{}, error) {
	if !table.IsIndexed(fieldName) {
		table.log.Error("Field '%s' is not indexed", fieldName)
		return nil, fmt.Errorf("Field '%s' is not indexed", fieldName)
	}

	indexValueBytes, err := gobEncode(value)
	if err != nil {
		table.log.Error("Error encoding index value: %s", err.Error())
		return nil, err
	}

	var primaryKeysData []byte
	err = table.data.View(func(tx *bolt.Tx) error {
		indexBucket := tx.Bucket([]byte("index:" + fieldName))
		primaryKeysData = indexBucket.Get(indexValueBytes)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if primaryKeysData == nil {
		table.log.Debug("Index value returned no primary keys")
		return []interface{}{}, nil
	}

	keys, err := gobDecodePrimaryKeyList(primaryKeysData)
	if err != nil {
		table.log.Error("Error decoding primary key list: %s", err.Error())
		return nil, err
	}

	var values = make([]interface{}, len(keys))
	for i, key := range keys {
		v, err := table.getPrimaryKey(key)
		if err != nil {
			table.log.Error("Error getting object: %s", err.Error())
			return nil, err
		}
		values[i] = v
	}

	return values, nil
}

// GetUnique will get a single entry based on the value of the provided unique field.
// Returns (nil, nil) if nothing found.
func (table *Table) GetUnique(fieldName string, value interface{}) (interface{}, error) {
	if !table.IsUnique(fieldName) {
		table.log.Error("Field '%s' is not unique", fieldName)
		return nil, fmt.Errorf("Field '%s' is not unique", fieldName)
	}

	uniqueValueBytes, err := gobEncode(value)
	if err != nil {
		table.log.Error("Error encoding unique value: %s", err.Error())
		return nil, err
	}

	var primaryKeyData []byte
	err = table.data.View(func(tx *bolt.Tx) error {
		uniqueBucket := tx.Bucket([]byte("unique:" + fieldName))
		primaryKeyData = uniqueBucket.Get(uniqueValueBytes)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if primaryKeyData == nil {
		table.log.Debug("Unique value returned no primary key")
		return nil, nil
	}

	return table.getPrimaryKey(primaryKeyData)
}

// GetAll will get all of the entries in the table.
func (table *Table) GetAll() ([]interface{}, error) {
	var entires []interface{}
	err := table.data.View(func(tx *bolt.Tx) error {
		dataBucket := tx.Bucket([]byte("data"))
		return dataBucket.ForEach(func(k []byte, v []byte) error {
			value, err := table.gobDecodeValue(v)
			if err != nil {
				table.log.Error("Error decoding value: %s", err.Error())
				return err
			}
			entires = append(entires, value.Interface())
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return entires, nil
}
