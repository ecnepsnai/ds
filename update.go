package ds

import (
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

func (table *Table) update(o any) error {
	if typeOf := reflect.TypeOf(o); typeOf.Kind() == reflect.Ptr {
		table.log.Error("Refusing to update pointer from table")
		return fmt.Errorf(ErrPointer)
	}

	// Check for an existing object, if nothing found then just add it and call it a day
	primaryKeyBytes, err := table.primaryKeyBytes(o)
	if err != nil {
		return err
	}
	existing, err := table.getPrimaryKey(primaryKeyBytes)
	if err != nil {
		return err
	}

	err = table.data.Update(func(tx *bbolt.Tx) error {
		if existing == nil {
			return table.addObject(tx, o)
		}

		var index *uint64
		if !table.options.DisableSorting {
			i, err := table.indexForObject(tx, o)
			if err != nil {
				return err
			}
			index = i
		}

		if err := table.deleteObject(tx, o); err != nil {
			return err
		}
		if err := table.addObject(tx, o); err != nil {
			return err
		}

		if !table.options.DisableSorting {
			if index != nil {
				// Reset the index for the re-added object
				if err := table.setInsertIndexForObject(tx, reflect.Indirect(reflect.ValueOf(o)), *index); err != nil {
					return err
				}
			}

			config, err := table.getConfig(tx)
			if err != nil {
				return err
			}
			// Decrement the last insert index since we've re-used an older index
			config.LastInsertIndex = config.LastInsertIndex - 1
			if err := config.update(tx); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		table.log.Error("Error updating entry: %s", err.Error())
		return err
	}

	return nil
}
