package ds_test

import (
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
)

// Test that you can delete a single object
func TestDelete(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	object := exampleType{
		Primary: randomString(12),
		Index:   randomString(12),
		Unique:  randomString(12),
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		err = tx.Add(object)
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		if err := tx.Delete(object); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}

		ret, err := tx.Get(object.Primary)
		if err != nil {
			t.Errorf("Error getting value from table: %s", err.Error())
		}
		if ret != nil {
			t.Error("Unexpected data returned for deleted object")
		}
		return nil
	})
}

// Test that you can delete many objects by an indexed value
func TestDeleteIndex(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		index := randomString(12)
		i := 0
		for i < 10 {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Unique:  randomString(12),
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		if err := tx.DeleteAllIndex("Index", index); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}
		return nil
	})
}

// Test that  nothing happens when deleting by index that doesn't match
func TestDeleteIndexMissing(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		index := randomString(12)
		i := 0
		for i < 10 {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Unique:  randomString(12),
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		if err := tx.DeleteAllIndex("Index", randomString(12)); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}

		objects, err := tx.GetIndex("Index", index, nil)
		if err != nil {
			t.Errorf("Error getting objects by index: %s", err.Error())
		}
		if len(objects) != 10 {
			t.Errorf("Unexpected number of objects returned. Expected 10 got %d", len(objects))
		}
		return nil
	})
}

// Test that deleting an object that is not in the table does not cause an error
func TestDeleteNotSaved(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		object := exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  randomString(12),
		}
		if err := tx.Delete(object); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}
		return nil
	})
}

// Test that you can delete an object by its primary key's value
func TestDeletePrimaryKey(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		object := exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  randomString(12),
		}

		err = tx.Add(object)
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		if err := tx.DeletePrimaryKey(object.Primary); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}

		ret, err := tx.Get(object.Primary)
		if err != nil {
			t.Errorf("Error getting value from table: %s", err.Error())
		}
		if ret != nil {
			t.Error("Unexpected data returned for deleted object")
		}
		return nil
	})
}

// Test that nothing happens when deleting an object with a primary key that doesn't match anything
func TestDeletePrimaryKeyMissing(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		if err := tx.DeletePrimaryKey(randomString(12)); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}
		return nil
	})
}

// Test that you can delete an object by any unique field's value
func TestDeleteUnique(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		object := exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  randomString(12),
		}

		err = tx.Add(object)
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		if err := tx.DeleteUnique("Unique", object.Unique); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}

		ret, err := tx.Get(object.Primary)
		if err != nil {
			t.Errorf("Error getting value from table: %s", err.Error())
		}
		if ret != nil {
			t.Error("Unexpected data returned for deleted object")
		}
		return nil
	})
}

// Test that nothing happens when deleting an object with a unique value that doesn't match anything
func TestDeleteUniqueMissing(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		if err := tx.DeleteUnique("Unique", randomString(12)); err != nil {
			t.Errorf("Error removing value from table: %s", err.Error())
		}
		return nil
	})
}

// Test that you can delete all data from the table
func TestDeleteAll(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		object := exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  randomString(12),
		}

		err = tx.Add(object)
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		err = tx.DeleteAll()
		if err != nil {
			t.Errorf("Error deleting all from table: %s", err.Error())
		}

		ret, err := tx.Get(object.Primary)
		if err != nil {
			t.Errorf("Error getting value from table: %s", err.Error())
		}
		if ret != nil {
			t.Error("Unexpected data returned for deleted object")
		}
		return nil
	})
}
