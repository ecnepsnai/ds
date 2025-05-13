package ds

import (
	"encoding/binary"
	"fmt"
	"sort"

	"go.etcd.io/bbolt"
)

// GetOptions describes options for getting entries from a DS table
type GetOptions struct {
	// Should the results be sorted. Does nothing for unsorted tables
	Sorted bool
	// If results are to be sorted, should they be from most recent to oldest (true) or invese (false)
	Ascending bool
	// The maximum number of entries to return. 0 means unlimited
	Max int
}

func (table *Table[T]) get(primaryKey any) (*T, error) {
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

func (table *Table[T]) getPrimaryKey(key []byte) (*T, error) {
	var data []byte
	err := table.data.View(func(tx *bbolt.Tx) error {
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

	return value, nil
}

func (table *Table[T]) getIndex(fieldName string, value any, options *GetOptions) ([]*T, error) {
	if !table.IsIndexed(fieldName) {
		table.log.Error("Field '%s' is not indexed", fieldName)
		return nil, fmt.Errorf("%s: %s", ErrFieldNotIndexed, fieldName)
	}

	o := GetOptions{}
	if options != nil {
		o = *options
	}

	if table.options.DisableSorting && o.Sorted {
		table.log.Warn("Requested sorted results from unsorted table")
		o.Sorted = false
	}

	indexValueBytes, err := gobEncode(value)
	if err != nil {
		table.log.Error("Error encoding index value: %s", err.Error())
		return nil, err
	}
	table.log.Debug("Get Index(%s) == %x", fieldName, indexValueBytes)

	var primaryKeysData []byte
	err = table.data.View(func(tx *bbolt.Tx) error {
		indexBucket := tx.Bucket([]byte(indexPrefix + fieldName))
		primaryKeysData = indexBucket.Get(indexValueBytes)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if primaryKeysData == nil {
		table.log.Debug("Index value returned no primary keys")
		return []*T{}, nil
	}

	keys, err := gobDecodePrimaryKeyList(primaryKeysData)
	if err != nil {
		table.log.Error("Error decoding primary key list: %s", err.Error())
		return nil, err
	}

	table.log.Debug("Keys matching Index(%s) == %x: %d", fieldName, indexValueBytes, len(keys))
	table.data.View(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(dataKey)
		i := len(keys) - 1
		for i >= 0 {
			pk := keys[i]
			if dataBucket.Get(pk) == nil {
				table.log.Warn("Removing unmatched index value for field '%s'", fieldName)
				keys = append(keys[:i], keys[i+1:]...)
			}
			i--
		}
		return nil
	})

	if o.Sorted {
		return table.getIndexSorted(keys, o)
	}
	return table.getIndexUnsorted(keys, o)
}

func (table *Table[T]) getIndexUnsorted(keys [][]byte, options GetOptions) ([]*T, error) {
	length := len(keys)
	if options.Max > 0 && length > options.Max {
		length = options.Max
	}

	var values = make([]*T, length)
	for i, key := range keys {
		if i >= length {
			break
		}

		v, err := table.getPrimaryKey(key)
		if err != nil {
			table.log.Error("Error getting object: %s", err.Error())
			return nil, err
		}
		values[i] = v
	}

	return values, nil
}

func (table *Table[T]) getIndexSorted(keys [][]byte, options GetOptions) ([]*T, error) {
	orderMap := map[uint64][]byte{}
	err := table.data.View(func(tx *bbolt.Tx) error {
		for _, key := range keys {
			index := table.indexForPrimaryKey(tx, key)
			if index == nil {
				return fmt.Errorf("no index found")
			}
			orderMap[*index] = key
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// To store the keys in slice in sorted order
	var indexes []uint64
	for k := range orderMap {
		indexes = append(indexes, k)
	}
	sort.SliceStable(indexes, func(i int, j int) bool {
		if options.Ascending {
			return indexes[i] > indexes[j]
		}
		return indexes[i] < indexes[j]
	})

	length := len(keys)
	if options.Max > 0 && length > options.Max {
		length = options.Max
	}
	var sortedObject = make([]*T, length)
	for i, key := range indexes {
		if i >= length {
			break
		}
		primaryKey := orderMap[key]
		o, err := table.getPrimaryKey(primaryKey)
		if err != nil {
			return nil, err
		}
		sortedObject[i] = o
	}

	return sortedObject, nil
}

func (table *Table[T]) getUnique(fieldName string, value any) (*T, error) {
	if !table.IsUnique(fieldName) {
		table.log.Error("Field '%s' is not unique", fieldName)
		return nil, fmt.Errorf("%s: %s", ErrFieldNotUnique, fieldName)
	}

	uniqueValueBytes, err := gobEncode(value)
	if err != nil {
		table.log.Error("Error encoding unique value: %s", err.Error())
		return nil, err
	}

	var primaryKeyData []byte
	err = table.data.View(func(tx *bbolt.Tx) error {
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

func (table *Table[T]) getAll(options *GetOptions) ([]*T, error) {
	o := GetOptions{}
	if options != nil {
		o = *options
	}

	if o.Sorted && table.options.DisableSorting {
		table.log.Warn("Requested sorted results from unsorted table")
		o.Sorted = false
	}

	if o.Sorted {
		return table.getAllSorted(o)
	}
	return table.getAllUnsorted(o)
}

func (table *Table[T]) getAllUnsorted(options GetOptions) ([]*T, error) {
	var entires []*T
	i := 0
	err := table.data.View(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(dataKey)
		return dataBucket.ForEach(func(k []byte, v []byte) error {
			i++
			if options.Max > 0 && i > options.Max {
				return nil
			}

			value, err := table.gobDecodeValue(v)
			if err != nil {
				table.log.Error("Error decoding value: %s", err.Error())
				return err
			}
			entires = append(entires, value)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return entires, nil
}

func (table *Table[T]) getAllSorted(options GetOptions) ([]*T, error) {
	// Map index to primary key
	orderMap := map[uint64][]byte{}
	err := table.data.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(insertOrderKey)
		return bucket.ForEach(func(k []byte, v []byte) error {
			primaryKey := k
			index := binary.LittleEndian.Uint64(v)
			orderMap[index] = primaryKey
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	if len(orderMap) == 0 {
		return []*T{}, nil
	}

	// To store the keys in slice in sorted order
	var keys []uint64
	for k := range orderMap {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i int, j int) bool {
		if options.Ascending {
			return keys[i] > keys[j]
		}
		return keys[i] < keys[j]
	})

	length := len(keys)
	if options.Max > 0 && length > options.Max {
		length = options.Max
	}
	objects := make([]*T, length)
	for i, index := range keys {
		if i >= length {
			break
		}

		primaryKey := orderMap[index]
		object, err := table.getPrimaryKey(primaryKey)
		if err != nil {
			return nil, err
		}
		objects[i] = object
	}

	return objects, nil
}
