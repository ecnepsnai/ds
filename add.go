package ds

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

func (table *Table[T]) add(o T) error {
	typeOf := reflect.TypeOf(o)
	if typeOf.Kind() == reflect.Ptr {
		table.log.Error("Refusing to add a pointer to the table")
		return errors.New(ErrPointer)
	}
	if err := compareFields(table.getFields(), structFieldsToFields(allFields(typeOf))); err != nil {
		table.log.Error("Incompatible object definition: %s", err.Error())
		return err
	}

	err := table.data.Update(func(tx *bbolt.Tx) error {
		table.log.Debug("Adding value to table")
		return table.addObject(tx, o)
	})
	if err != nil {
		return err
	}

	return nil
}

func (table *Table[T]) addUpdateIndex(tx *bbolt.Tx, valueOf reflect.Value, primaryKeyBytes []byte) error {
	for _, index := range table.indexes {
		indexValue := valueOf.FieldByName(index)
		indexValueBytes, err := gobEncode(indexValue.Interface())
		if err != nil {
			table.log.PError("Error encoding value for field", map[string]any{
				"index": index,
				"error": err.Error(),
			})
			return err
		}

		table.log.Debug("Index(%s) value: %x", index, indexValueBytes)

		indexBucket := tx.Bucket([]byte(indexPrefix + index))
		var primaryKeys [][]byte
		if data := indexBucket.Get(indexValueBytes); data != nil {
			pk, err := gobDecodePrimaryKeyList(data)
			if err != nil {
				table.log.PError("Error decoding primary key list for index", map[string]any{
					"index": index,
					"error": err.Error(),
				})
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
			table.log.PError("Error encoding primary key list for index", map[string]any{
				"index": index,
				"error": err.Error(),
			})
			return err
		}
		if err := indexBucket.Put(indexValueBytes, pkListBytes); err != nil {
			table.log.PError("Error updating index", map[string]any{
				"index": index,
				"error": err.Error(),
			})
			return err
		}
	}

	return nil
}

func (table *Table[T]) addUpdateUnique(tx *bbolt.Tx, valueOf reflect.Value, primaryKeyBytes []byte) error {
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

		table.log.Debug("Unique(%s) value: %x", unique, uniqueValueBytes)

		uniqueBucket := tx.Bucket([]byte(uniquePrefix + unique))
		if data := uniqueBucket.Get(uniqueValueBytes); data != nil {
			// Check that if there is a duplicate unique value that it actually maps to data.
			// If it doesn't, delete the unmatched value
			if tx.Bucket(dataKey).Get(data) != nil {
				table.log.PError("Duplicate value for unique field", map[string]any{
					"field": unique,
					"value": uniqueValueBytes,
				})
				return fmt.Errorf("%s: %s", ErrDuplicateUnique, unique)
			} else {
				if err := uniqueBucket.Delete(uniqueValueBytes); err != nil {
					table.log.PError("Failed to correct unmatched unique value", map[string]any{
						"field": unique,
						"value": uniqueValueBytes,
						"error": err.Error(),
					})
					return err
				}
				table.log.PWarn("Corrected unmatched duplicate unique value", map[string]any{
					"field": unique,
				})
			}
		}
		table.log.Debug("Updating unique '%s'", unique)
		if err := uniqueBucket.Put(uniqueValueBytes, primaryKeyBytes); err != nil {
			table.log.PError("Error updating unique", map[string]any{
				"field": unique,
				"error": err.Error(),
			})
			return err
		}
	}

	return nil
}

func (table *Table[T]) addObject(tx *bbolt.Tx, o any) error {
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
		return errors.New(ErrDuplicatePrimaryKey)
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
			table.log.PError("Error updating insert index for entry", map[string]any{
				"error": err.Error(),
			})
			return err
		}
		config.LastInsertIndex = index
		if err := config.update(tx); err != nil {
			table.log.PError("Error updating table config", map[string]any{
				"error": err.Error(),
			})
			return err
		}
	}

	if err := dataBucket.Put(primaryKeyBytes, data); err != nil {
		table.log.PError("Error inserting new object", map[string]any{
			"error": err.Error(),
		})
		return err
	}

	return nil
}

func (table *Table[T]) setInsertIndexForObject(tx *bbolt.Tx, object reflect.Value, index uint64) error {
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
