package ds

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

func (table *Table) delete(o any) error {
	typeOf := reflect.TypeOf(o)

	if typeOf.Kind() == reflect.Ptr {
		table.log.Error("Refusing to delete a pointer from a table")
		return fmt.Errorf(ErrPointer)
	} else if table.typeOf.Name() != typeOf.Name() {
		table.log.Error("Cannot delete type '%s' from table registered for type '%s'", typeOf.Name(), table.typeOf.Name())
		return fmt.Errorf("cannot delete type '%s' from table registered for type '%s'", typeOf.Name(), table.typeOf.Name())
	}

	err := table.data.Update(func(tx *bbolt.Tx) error {
		return table.deleteObject(tx, o)
	})
	if err != nil {
		return err
	}

	return nil
}

func (table *Table) deletePrimaryKey(o any) error {
	object, err := table.get(o)
	if err != nil {
		return err
	}
	if object == nil {
		return nil
	}
	return table.delete(object)
}

func (table *Table) deleteUnique(field string, o any) error {
	object, err := table.getUnique(field, o)
	if err != nil {
		return err
	}
	if object == nil {
		return nil
	}
	return table.delete(object)
}

func (table *Table) deleteObject(tx *bbolt.Tx, o any) error {
	primaryKeyBytes, err := table.primaryKeyBytes(o)
	if err != nil {
		return err
	}

	dataBucket := tx.Bucket(dataKey)
	dataBucket.Delete(primaryKeyBytes)

	for _, indexField := range table.indexes {
		indexBucket := tx.Bucket([]byte(indexPrefix + indexField))

		indexesToUpdate := map[string]int{}

		err = indexBucket.ForEach(func(k []byte, v []byte) error {
			primaryKeys, err := gobDecodePrimaryKeyList(v)
			if err != nil {
				table.log.PError("Error decoding primary key list for index", map[string]any{
					"index": indexField,
					"key":   k,
					"error": err.Error(),
				})
				return err
			}

			pkIndex := -1
			for i, primaryKey := range primaryKeys {
				if bytes.Equal(primaryKey, primaryKeyBytes) {
					pkIndex = i
				}
			}
			if pkIndex == -1 {
				table.log.Debug("Primary key not found in Index(%s:%x)", indexField, k)
				return nil
			}

			indexesToUpdate[hex.EncodeToString(k)] = pkIndex
			return nil
		})
		if err != nil {
			return err
		}

		for k, idx := range indexesToUpdate {
			key, _ := hex.DecodeString(k)
			data := indexBucket.Get(key)
			primaryKeys, err := gobDecodePrimaryKeyList(data)
			if err != nil {
				table.log.PError("Error decoding primary key list for index", map[string]any{
					"index": indexField,
					"key":   k,
					"error": err.Error(),
				})
				return err
			}

			if len(primaryKeys) == 1 {
				if err := indexBucket.Delete(key); err != nil {
					table.log.PError("Error removing index", map[string]any{
						"index": indexField,
						"key":   k,
						"error": err.Error(),
					})
					return err
				}
				table.log.Debug("Updating Index(%s:%x). Key count: 0", indexField, k)
				continue
			}

			primaryKeys = append(primaryKeys[:idx], primaryKeys[idx+1:]...)
			table.log.Debug("Updating Index(%s:%x). Key count: %d", indexField, k, len(primaryKeys))
			pkListBytes, err := gobEncode(primaryKeys)
			if err != nil {
				table.log.PError("Error encoding primary key list for index", map[string]any{
					"index": indexField,
					"key":   k,
					"error": err.Error(),
				})
				return err
			}
			if err := indexBucket.Put(key, pkListBytes); err != nil {
				table.log.PError("Error updating index", map[string]any{
					"index": indexField,
					"key":   k,
					"error": err.Error(),
				})
				return err
			}
		}
	}

	valueOf := reflect.Indirect(reflect.ValueOf(o))
	for _, unique := range table.uniques {
		uniqueValue := valueOf.FieldByName(unique)
		uniqueValueBytes, err := gobEncode(uniqueValue.Interface())
		if err != nil {
			table.log.PError("Error encoding value for field", map[string]any{
				"field": unique,
				"error": err.Error(),
			})
			return err
		}

		uniqueBucket := tx.Bucket([]byte(uniquePrefix + unique))
		table.log.Debug("Updating unique '%s'", unique)
		if err := uniqueBucket.Delete(uniqueValueBytes); err != nil {
			table.log.PError("Error removing unique", map[string]any{
				"field": unique,
				"error": err.Error(),
			})
			return err
		}
	}

	if !table.options.DisableSorting {
		indexBucket := tx.Bucket(insertOrderKey)
		indexBucket.Delete(primaryKeyBytes)
	}

	return nil
}

func (table *Table) deleteAllIndex(fieldName string, value any) error {
	objects, err := table.getIndex(fieldName, value, nil)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := table.delete(object); err != nil {
			return err
		}
	}

	return nil
}

func (table *Table) deleteAll() error {
	return table.data.Update(func(tx *bbolt.Tx) error {
		if err := table.purgeBucket(tx, dataKey); err != nil {
			return err
		}
		if err := table.purgeBucket(tx, insertOrderKey); err != nil {
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
}

func (table *Table) purgeBucket(tx *bbolt.Tx, bucketName []byte) error {
	if err := tx.DeleteBucket(bucketName); err != nil {
		table.log.PError("Error deleting bucket", map[string]any{
			"bucket": bucketName,
			"error":  err.Error(),
		})
		return err
	}
	table.log.Debug("Deleting bucket '%s'", bucketName)
	_, err := tx.CreateBucket(bucketName)
	if err != nil {
		table.log.PError("Error creating bucket", map[string]any{
			"bucket": bucketName,
			"error":  err.Error(),
		})
		return err
	}
	table.log.Warn("Bucket '%s' purged", bucketName)
	return nil
}
