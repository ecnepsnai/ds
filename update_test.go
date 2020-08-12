package ds_test

import (
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
)

// Test that a new entry will still be added with an update
func TestUpdateNewValue(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
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

	table, err := ds.Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
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

	objs, err := table.GetAll(&ds.GetOptions{Sorted: true})
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

	table, err := ds.Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
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
