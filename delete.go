package ds

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/boltdb/bolt"
)

// Delete will delete the provided object and clean indexes
func (table *Table) Delete(o interface{}) error {
	typeOf := reflect.TypeOf(o)
	if table.typeOf.Name() != typeOf.Name() {
		table.log.Error("Cannot add type '%s' to table registered for type '%s'", typeOf.Name(), table.typeOf.Name())
		return fmt.Errorf("Cannot add type '%s' to table registered for type '%s'", typeOf.Name(), table.typeOf.Name())
	}

	err := table.data.Update(func(tx *bolt.Tx) error {
		return table.delete(tx, o)
	})
	if err != nil {
		return err
	}

	return nil
}

// DeletePrimaryKey will delete the object with the associated primary key and clean indexes
func (table *Table) DeletePrimaryKey(o interface{}) error {
	object, err := table.Get(o)
	if err != nil {
		return err
	}
	return table.Delete(object)
}

// DeleteUnique will delete the object with the associated unique value and clean indexes
func (table *Table) DeleteUnique(field string, o interface{}) error {
	object, err := table.GetUnique(field, o)
	if err != nil {
		return err
	}
	return table.Delete(object)
}

func (table *Table) delete(tx *bolt.Tx, o interface{}) error {
	valueOf := reflect.Indirect(reflect.ValueOf(o))
	primaryKeyValue := valueOf.FieldByName(table.primaryKey)
	primaryKeyBytes, err := gobEncode(primaryKeyValue.Interface())
	if err != nil {
		table.log.Error("Cannot encode primary key value: %s", err.Error())
		return err
	}

	dataBucket := tx.Bucket(dataKey)
	dataBucket.Delete(primaryKeyBytes)

	for _, index := range table.indexes {
		indexValue := valueOf.FieldByName(index)
		indexValueBytes, err := gobEncode(indexValue.Interface())
		if err != nil {
			table.log.Error("Error encoding value for field '%s': %s", index, err.Error())
			return err
		}

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
			continue
		}

		if len(primaryKeys) == 1 {
			if err := indexBucket.Delete(indexValueBytes); err != nil {
				table.log.Error("Error removing index '%s': %s", index, err.Error())
				return err
			}
			table.log.Debug("Updating index '%s'. Key count: 0", index)
			continue
		}

		pkIndex := indexOf(primaryKeys, primaryKeyBytes)
		if pkIndex == -1 {
			table.log.Warn("Primary key not found in index '%s'", index)
			continue
		}
		primaryKeys[pkIndex] = primaryKeys[len(primaryKeys)-1]
		primaryKeys = primaryKeys[:len(primaryKeys)-1]
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

		uniqueBucket := tx.Bucket([]byte(uniquePrefix + unique))
		table.log.Debug("Updating unique '%s'", unique)
		if err := uniqueBucket.Delete(uniqueValueBytes); err != nil {
			table.log.Error("Error removing unique '%s': %s", unique, err.Error())
			return err
		}
	}

	if !table.options.DisableSorting {
		indexBucket := tx.Bucket(insertOrderKey)
		indexBucket.Delete(primaryKeyBytes)
	}

	return nil
}

// DeleteAllIndex will delete all objects matching the given indexed fields value
func (table *Table) DeleteAllIndex(fieldName string, value interface{}) error {
	objects, err := table.GetIndex(fieldName, value, nil)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := table.Delete(object); err != nil {
			return err
		}
	}

	return nil
}

func indexOf(slice [][]byte, value []byte) int {
	for i, b := range slice {
		if bytes.Compare(b, value) == 0 {
			return i
		}
	}

	return -1
}

// DeleteAll delete all objects from the table
func (table *Table) DeleteAll() error {
	err := table.data.Update(func(tx *bolt.Tx) error {
		if err := table.purgeBucket(tx, dataKey); err != nil {
			return err
		}
		for _, index := range table.indexes {
			if err := table.purgeBucket(tx, []byte(indexPrefix+index)); err != nil {
				return err
			}
		}
		for _, unique := range table.uniques {
			if err := table.purgeBucket(tx, []byte(uniquePrefix+unique)); err != nil {
				return err
			}
		}
		config, err := table.getConfig(tx)
		if err != nil {
			return err
		}
		config.LastInsertIndex = 0
		if err := config.update(tx); err != nil {
			return err
		}

		return nil
	})
	return err
}

func (table *Table) purgeBucket(tx *bolt.Tx, bucketName []byte) error {
	if err := tx.DeleteBucket(bucketName); err != nil {
		table.log.Error("Error deleting bucket '%s': %s", bucketName, err.Error())
		return err
	}
	table.log.Debug("Deleting bucket '%s'", bucketName)
	_, err := tx.CreateBucket(bucketName)
	if err != nil {
		table.log.Error("Error creating bucket '%s': %s", bucketName, err.Error())
		return err
	}
	table.log.Warn("Bucket '%s' purged", bucketName)
	return nil
}
