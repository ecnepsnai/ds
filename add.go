package ds

import (
	"fmt"
	"reflect"

	"github.com/boltdb/bolt"
)

// Add will add a new object to the table. o must the the same type that was used to register the table and cannot be a pointer.
func (table *Table) Add(o interface{}) error {
	typeOf := reflect.TypeOf(o)
	if table.typeOf.Name() != typeOf.Name() {
		table.log.Error("Cannot add type '%s' to table registered for type '%s'", typeOf.Name(), table.typeOf.Name())
		return fmt.Errorf("Cannot add type '%s' to table registered for type '%s'", typeOf.Name(), table.typeOf.Name())
	}

	valueOf := reflect.Indirect(reflect.ValueOf(o))
	primaryKeyValue := valueOf.FieldByName(table.primaryKey)
	primaryKeyBytes, err := gobEncode(primaryKeyValue.Interface())
	if err != nil {
		table.log.Error("Cannot encode primary key value: %s", err.Error())
		return err
	}

	err = table.data.Update(func(tx *bolt.Tx) error {
		dataBucket := tx.Bucket([]byte("data"))
		if data := dataBucket.Get(primaryKeyBytes); data != nil {
			table.log.Error("Duplicate primary key")
			return fmt.Errorf("Duplicate primary key")
		}

		for _, index := range table.indexes {
			indexValue := valueOf.FieldByName(index)
			indexValueBytes, err := gobEncode(indexValue.Interface())
			if err != nil {
				table.log.Error("Error encoding value for field '%s': %s", index, err.Error())
				return err
			}

			table.log.Debug("Index(%s) value: %x", index, indexValueBytes)

			indexBucket := tx.Bucket([]byte("index:" + index))
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
		for _, unique := range table.uniques {
			uniqueValue := valueOf.FieldByName(unique)
			uniqueValueBytes, err := gobEncode(uniqueValue.Interface())
			if err != nil {
				table.log.Error("Error encoding value for field '%s': %s", unique, err.Error())
				return err
			}

			table.log.Debug("Unique(%s) value: %x", unique, uniqueValueBytes)

			uniqueBucket := tx.Bucket([]byte("unique:" + unique))
			if data := uniqueBucket.Get(uniqueValueBytes); data != nil {
				table.log.Error("Non-unique value for unique field '%s'", unique)
				return fmt.Errorf("Non-unique value for unique field '%s'", unique)
			}
			table.log.Debug("Updating unique '%s'", unique)
			if err := uniqueBucket.Put(uniqueValueBytes, primaryKeyBytes); err != nil {
				table.log.Error("Error updating unique '%s': %s", unique, err.Error())
				return err
			}
		}

		data, err := gobEncode(o)
		if err != nil {
			table.log.Error("Error encoding object: %s", err.Error())
			return err
		}
		if err := dataBucket.Put(primaryKeyBytes, data); err != nil {
			table.log.Error("Error inserting new object: %s", err.Error())
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
