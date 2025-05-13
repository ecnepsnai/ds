package ds_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
)

// Test that existing values can be updated in a table
func TestUpdateExistingValue(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   int
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		count := 5

		i := 0
		for i < count {
			object := exampleType{
				Primary: fmt.Sprintf("%d", i),
				Index:   i,
				Unique:  randomString(12),
			}

			err = tx.Add(object)
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		i = count - 1
		for i >= 0 {
			if err := tx.Update(exampleType{
				Primary: fmt.Sprintf("%d", i),
				Index:   i,
				Unique:  randomString(12),
			}); err != nil {
				t.Errorf("Error updating existing value: %s", err.Error())
			}
			i--
		}

		results, err := tx.GetAll(&ds.GetOptions{
			Sorted: true,
		})
		if err != nil {
			t.Errorf("Error getting all values from table: %s", err.Error())
		}

		for i, result := range results {
			if result.Index != i {
				t.Errorf("Incorrect index of updated object. Expected %d got %d", i, result.Index)
			}
		}
		return nil
	})
}

// Test that a new entry will still be added with an update
func TestUpdateNewValue(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		object := exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  randomString(12),
		}

		err = tx.Update(object)
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		return nil
	})
}

// Test that many new entries will still be added with an update
func TestUpdateManyNewValue(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary int `ds:"primary"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		expected := 10

		i := 0
		for i < expected {
			if err := tx.Update(exampleType{i}); err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		objs, err := tx.GetAll(&ds.GetOptions{Sorted: true})
		if err != nil {
			t.Errorf("Error getting all from table: %s", err.Error())
		}

		result := len(objs)
		if result != expected {
			t.Errorf("Unexpected length. Expected %d got %d", expected, result)
		}
		return nil
	})
}
