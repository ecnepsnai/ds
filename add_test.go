package ds_test

import (
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
)

// Test that you can add a row to the table
func TestAdd(t *testing.T) {
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

	err = table.Add(exampleType{
		Primary: randomString(12),
		Index:   randomString(12),
		Unique:  randomString(12),
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}
}

// Test that you can add a row with an indexed field to the table
func TestAddIndex(t *testing.T) {
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

	index := randomString(12)
	i := 0
	for i < 10 {
		err = table.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Unique:  randomString(12),
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}
}

// Test that you can't add an object of a different type to a table
func TestAddTypeMismatch(t *testing.T) {
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

	type otherType struct {
		Foo string `ds:"primary"`
	}

	err = table.Add(otherType{
		Foo: randomString(12),
	})
	if err == nil {
		t.Error("No error seen while attempting to insert incorrect object into table")
	}
}

// Test that you can't add a pointer to a table
func TestAddPointer(t *testing.T) {
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

	type otherType struct {
		Foo string `ds:"primary"`
	}

	err = table.Add(&otherType{
		Foo: randomString(12),
	})
	if err == nil {
		t.Error("No error seen while attempting to insert pointer into table")
	}
}

// Test that you can't add an object with a duplicate primary key value
func TestAddDuplicatePrimaryKey(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	primaryKey := randomString(12)

	table, err := ds.Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.Add(exampleType{
		Primary: primaryKey,
		Index:   randomString(12),
		Unique:  randomString(12),
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	err = table.Add(exampleType{
		Primary: primaryKey,
		Index:   randomString(12),
		Unique:  randomString(12),
	})
	if err == nil {
		t.Errorf("No error seen while attempting to insert object with duplicate primary key")
	}
}

// Test that you can't add an object with a duplicate unique fields value
func TestAddDuplicateUnique(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	unique := randomString(12)

	table, err := ds.Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.Add(exampleType{
		Primary: randomString(12),
		Index:   randomString(12),
		Unique:  unique,
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	err = table.Add(exampleType{
		Primary: randomString(12),
		Index:   randomString(12),
		Unique:  unique,
	})
	if err == nil {
		t.Errorf("No error seen while attempting to insert object with duplicate primary key")
	}
}
