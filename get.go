package ds

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"

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
		dataBucket := tx.Bucket(dataKey)
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
// Result is not ordered. Use GetIndexSorted to return a sorted slice.
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
		indexBucket := tx.Bucket([]byte(indexPrefix + fieldName))
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

// GetIndexSorted will get multiple entries that contain the same value for the specified indexed field
// sorted by their insertion.
// If ascending is true, the results are sorted by most recently inserted to oldest. If false, it's the reverse.
// Retuns an empty array if nothing found.
func (table *Table) GetIndexSorted(fieldName string, value interface{}, ascending bool) ([]interface{}, error) {
	if table.options.DisableSorting {
		table.log.Error("Call GetIndexSorted on non-sorted table")
		return nil, fmt.Errorf("Call GetIndexSorted on non-sorted table")
	}

	objects, err := table.GetIndex(fieldName, value)
	if err != nil {
		return nil, err
	}
	if len(objects) == 0 {
		return objects, nil
	}

	orderMap := map[uint64]interface{}{}
	err = table.data.View(func(tx *bolt.Tx) error {
		for _, object := range objects {
			index, err := table.indexForObject(tx, object)
			if err != nil {
				return err
			}
			orderMap[index] = object
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// To store the keys in slice in sorted order
	var keys []uint64
	for k := range orderMap {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i int, j int) bool {
		if ascending {
			return keys[i] > keys[j]
		}
		return keys[i] < keys[j]
	})

	var sortedObject = make([]interface{}, len(keys))
	for i, key := range keys {
		sortedObject[i] = orderMap[key]
	}

	return sortedObject, nil
}

func (table *Table) indexForObject(tx *bolt.Tx, object interface{}) (uint64, error) {
	pk := reflect.ValueOf(object).FieldByName(table.primaryKey).Interface()
	pkBytes, err := gobEncode(pk)
	if err != nil {
		return 0, err
	}
	indexBucket := tx.Bucket(insertOrderKey)
	b := indexBucket.Get(pkBytes)
	index := binary.LittleEndian.Uint64(b)
	if err != nil {
		return 0, err
	}
	return index, nil
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
		uniqueBucket := tx.Bucket([]byte(uniquePrefix + fieldName))
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
		dataBucket := tx.Bucket(dataKey)
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
