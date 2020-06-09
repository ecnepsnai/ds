package ds

import (
	"path"
	"testing"

	"go.etcd.io/bbolt"
)

// Test that an object can be updated
func TestUpdateExistingValue(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	object := exampleType{
		Primary: randomString(12),
		Index:   randomString(12),
		Unique:  randomString(12),
	}

	err = table.Add(object)
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	var index uint64
	var lastInsertIndex uint64
	table.data.View(func(tx *bbolt.Tx) error {
		i, err := table.indexForObject(tx, object)
		if err != nil {
			return err
		}
		index = *i

		config, err := table.getConfig(tx)
		if err != nil {
			return err
		}
		lastInsertIndex = config.LastInsertIndex

		return nil
	})
	if err != nil {
		t.Errorf("Error getting object insert index: %s", err.Error())
	}

	oldIndex := object.Index
	object.Index = randomString(12)

	err = table.Update(object)
	if err != nil {
		t.Errorf("Error updating value to table: %s", err.Error())
	}

	var newIndex uint64
	var newLastInsertIndex uint64
	table.data.View(func(tx *bbolt.Tx) error {
		i, err := table.indexForObject(tx, object)
		if err != nil {
			return err
		}
		newIndex = *i

		config, err := table.getConfig(tx)
		if err != nil {
			return err
		}
		newLastInsertIndex = config.LastInsertIndex

		return nil
	})
	if err != nil {
		t.Errorf("Error getting object insert index: %s", err.Error())
	}

	if newIndex != index {
		t.Errorf("Insert index is different from original index. Expected %d got %d", index, newIndex)
	}

	if newLastInsertIndex != lastInsertIndex {
		t.Errorf("Last Inserted Index is not expected. Expected %d got %d", lastInsertIndex, newLastInsertIndex)
	}

	// Try to get the object by its old index
	objects, err := table.GetIndex("Index", oldIndex, nil)
	if err != nil {
		t.Errorf("Error getting objects: %s", err.Error())
	}
	if len(objects) > 0 {
		// If this fails, then the indexes aren't being cleaned up correctly
		t.Errorf("Unexpected number of objects returned. Expected 0 got %d", len(objects))
	}
}

// Test that a new entry will still be added with an update
func TestUpdateNewValue(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	object := exampleType{
		Primary: randomString(12),
		Index:   randomString(12),
		Unique:  randomString(12),
	}

	err = table.Update(object)
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}
}

// Test that many new entries will still be added with an update
func TestUpdateManyNewValue(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary int `ds:"primary"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	expected := 10

	i := 0
	for i < expected {
		if err := table.Update(exampleType{i}); err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	objs, err := table.GetAll(&GetOptions{Sorted: true})
	if err != nil {
		t.Errorf("Error getting all from table: %s", err.Error())
	}

	result := len(objs)
	if result != expected {
		t.Errorf("Unexpected length. Expected %d got %d", expected, result)
	}
}

func TestUpdatePointer(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary int `ds:"primary"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	if err := table.Add(exampleType{1}); err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	err = table.Update(&exampleType{1})
	if err == nil {
		t.Errorf("No error seen when one expected when updating pointer")
	}
}
