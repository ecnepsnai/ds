package ds

import (
	"path"
	"testing"
)

// Test that you can delete a single object
func TestDelete(t *testing.T) {
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

	if err := table.Delete(object); err != nil {
		t.Errorf("Error removing value from table: %s", err.Error())
	}

	ret, err := table.Get(object.Primary)
	if err != nil {
		t.Errorf("Error getting value from table: %s", err.Error())
	}
	if ret != nil {
		t.Error("Unexpected data returned for deleted object")
	}
}

// Test that you can delete many objects by an indexed value
func TestDeleteIndex(t *testing.T) {
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

	if err := table.DeleteAllIndex("Index", index); err != nil {
		t.Errorf("Error removing value from table: %s", err.Error())
	}
}

// Test that deleting an object that is not in the table does not cause an error
func TestDeleteNotSaved(t *testing.T) {
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
	if err := table.Delete(object); err != nil {
		t.Errorf("Error removing value from table: %s", err.Error())
	}
}

func TestDeleteTypeMismatch(t *testing.T) {
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

	type otherType struct {
		Foo string `ds:"primary"`
	}

	err = table.Delete(otherType{
		Foo: randomString(12),
	})
	if err == nil {
		t.Error("No error seen while attempting to delete incorrect object into table")
	}
}

func TestDeletePrimaryKey(t *testing.T) {
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

	if err := table.DeletePrimaryKey(object.Primary); err != nil {
		t.Errorf("Error removing value from table: %s", err.Error())
	}

	ret, err := table.Get(object.Primary)
	if err != nil {
		t.Errorf("Error getting value from table: %s", err.Error())
	}
	if ret != nil {
		t.Error("Unexpected data returned for deleted object")
	}
}

func TestDeleteUnique(t *testing.T) {
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

	if err := table.DeleteUnique("Unique", object.Unique); err != nil {
		t.Errorf("Error removing value from table: %s", err.Error())
	}

	ret, err := table.Get(object.Primary)
	if err != nil {
		t.Errorf("Error getting value from table: %s", err.Error())
	}
	if ret != nil {
		t.Error("Unexpected data returned for deleted object")
	}
}

func TestDeleteAll(t *testing.T) {
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

	err = table.DeleteAll()
	if err != nil {
		t.Errorf("Error deleting all from table: %s", err.Error())
	}

	ret, err := table.Get(object.Primary)
	if err != nil {
		t.Errorf("Error getting value from table: %s", err.Error())
	}
	if ret != nil {
		t.Error("Unexpected data returned for deleted object")
	}
}
