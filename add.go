package ds

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

// Add will add a new object to the table. o must the the same type that was used to register the table and cannot be a pointer.
func (table *Table) Add(o interface{}) error {
	typeOf := reflect.TypeOf(o)
	if typeOf.Kind() == reflect.Ptr {
		table.log.Error("Refusing to add a pointer to the table")
		return fmt.Errorf("refusing to add a pointer to the table")
	}
	if err := compareFields(table.getFields(), structFieldsToFields(allFields(typeOf))); err != nil {
		table.log.Error("Incompatible object definition: %s", err.Error())
		return err
	}

	err := table.data.Update(func(tx *bbolt.Tx) error {
		table.log.Debug("Adding value to table")
		return table.add(tx, o)
	})
	if err != nil {
		return err
	}

	return nil
}

func (table *Table) addUpdateIndex(tx *bbolt.Tx, valueOf reflect.Value, primaryKeyBytes []byte) error {
	for _, index := range table.indexes {
		indexValue := valueOf.FieldByName(index)
		indexValueBytes, err := gobEncode(indexValue.Interface())
		if err != nil {
			table.log.Error("Error encoding value for field '%s': %s", index, err.Error())
			return err
		}

		table.log.Debug("Index(%s) value: %x", index, indexValueBytes)

		indexBucket := tx.Bucket([]byte(indexPrefix + index))
		var primaryKeys [][]byte
		if data := indexBucket.Get(indexValueBytes); data != nil {
			pk, err := gobDecodePrimaryKeyList(data)
			if err != nil {
				table.log.Error("Error decoding primary key list for index '%s': %s", index, err.Error())
				return err
			}
			primaryKeys = pk
		} else {
			primaryKeys = [][]byte{}
		}

		primaryKeys = append(primaryKeys, primaryKeyBytes)
		table.log.Debug("Updating index '%s'. Key count: %d", index, len(primaryKeys))
		pkListBytes, err := gobEncode(primaryKeys)
		if err != nil {
			table.log.Error("Error encoding primary key list for index '%s': %s", index, err.Error())
			return err
		}
		if err := indexBucket.Put(indexValueBytes, pkListBytes); err != nil {
			table.log.Error("Error updating index '%s': %s", index, err.Error())
			return err
		}
	}

	return nil
}

func (table *Table) addUpdateUnique(tx *bbolt.Tx, valueOf reflect.Value, primaryKeyBytes []byte) error {
	for _, unique := range table.uniques {
		uniqueValue := valueOf.FieldByName(unique)
		uniqueValueBytes, err := gobEncode(uniqueValue.Interface())
		if err != nil {
			table.log.Error("Error encoding value for field '%s': %s", unique, err.Error())
			return err
		}

		table.log.Debug("Unique(%s) value: %x", unique, uniqueValueBytes)

		uniqueBucket := tx.Bucket([]byte(uniquePrefix + unique))
		if data := uniqueBucket.Get(uniqueValueBytes); data != nil {
			// Check that if there is a duplicate unique value that it actually maps to data.
			// If it doesn't, delete the unmatched value
			if tx.Bucket(dataKey).Get(data) != nil {
				table.log.Error("Non-unique value for unique field '%s'", unique)
				return fmt.Errorf("non-unique value for unique field '%s'", unique)
			} else {
				if err := uniqueBucket.Delete(uniqueValueBytes); err != nil {
					table.log.Error("Failed to correct unmatched unique value for field '%s': %s", unique, err.Error())
					return err
				}
				table.log.Warn("Corrected unmatched duplicate unique value for field '%s'", unique)
			}
		}
		table.log.Debug("Updating unique '%s'", unique)
		if err := uniqueBucket.Put(uniqueValueBytes, primaryKeyBytes); err != nil {
			table.log.Error("Error updating unique '%s': %s", unique, err.Error())
			return err
		}
	}

	return nil
}

func (table *Table) add(tx *bbolt.Tx, o interface{}) error {
	valueOf := reflect.Indirect(reflect.ValueOf(o))
	primaryKeyValue := valueOf.FieldByName(table.primaryKey)
	primaryKeyBytes, err := gobEncode(primaryKeyValue.Interface())
	if err != nil {
		table.log.Error("Cannot encode primary key value: %s", err.Error())
		return err
	}
	data, err := gobEncode(o)
	if err != nil {
		table.log.Error("Error encoding object: %s", err.Error())
		return err
	}

	dataBucket := tx.Bucket(dataKey)
	if data := dataBucket.Get(primaryKeyBytes); data != nil {
		table.log.Error("Duplicate primary key")
		return fmt.Errorf("duplicate primary key")
	}

	if err := table.addUpdateIndex(tx, valueOf, primaryKeyBytes); err != nil {
		return err
	}
	if err := table.addUpdateUnique(tx, valueOf, primaryKeyBytes); err != nil {
		return err
	}

	if !table.options.DisableSorting {
		config, err := table.getConfig(tx)
		if err != nil {
			return err
		}

		index := config.LastInsertIndex + 1
		if err := table.setInsertIndexForObject(tx, valueOf, index); err != nil {
			table.log.Error("Error updating insert index for entry: %s", err.Error())
			return err
		}
		config.LastInsertIndex = index
		if err := config.update(tx); err != nil {
			table.log.Error("Error updating table config: %s", err.Error())
			return err
		}
	}

	if err := dataBucket.Put(primaryKeyBytes, data); err != nil {
		table.log.Error("Error inserting new object: %s", err.Error())
		return err
	}

	return nil
}

func (table *Table) setInsertIndexForObject(tx *bbolt.Tx, object reflect.Value, index uint64) error {
	pk := object.FieldByName(table.primaryKey).Interface()
	primaryKey, err := gobEncode(pk)
	if err != nil {
		return err
	}

	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, index)
	bucket := tx.Bucket(insertOrderKey)
	table.log.Debug("Setting insert index %d for entry %x", index, primaryKey)
	return bucket.Put(primaryKey, bs)
}
