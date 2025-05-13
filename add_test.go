package ds_test

import (
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
	"go.etcd.io/bbolt"
)

// Test that you can add a row to the table
func TestAdd(t *testing.T) {
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

	err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		return tx.Add(exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  randomString(12),
		})
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

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	index := randomString(12)
	i := 0
	for i < 10 {
		err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
			return tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Unique:  randomString(12),
			})
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
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

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		return tx.Add(exampleType{
			Primary: primaryKey,
			Index:   randomString(12),
			Unique:  randomString(12),
		})
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		return tx.Add(exampleType{
			Primary: primaryKey,
			Index:   randomString(12),
			Unique:  randomString(12),
		})
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

	table, err := ds.Register[exampleType](exampleType{}, path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		return tx.Add(exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  unique,
		})
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		return tx.Add(exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  unique,
		})
	})
	if err == nil {
		t.Errorf("No error seen while attempting to insert object with duplicate primary key")
	}
}

// Test that you can add an object into a table that has an unmatched unique field
func TestAddUnmatchedUnique(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Unique  string `ds:"unique"`
	}

	unique := randomString(12)

	tablePath := path.Join(t.TempDir(), randomString(12))
	table, err := ds.Register[exampleType](exampleType{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		return tx.Add(exampleType{
			Primary: randomString(12),
			Unique:  unique,
		})
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	table.Close()

	db, err := bbolt.Open(tablePath, os.ModePerm, nil)
	if err != nil {
		t.Fatalf("Error opening bolt table: %s", err.Error())
	}
	db.Update(func(tx *bbolt.Tx) error {
		tx.DeleteBucket([]byte("data"))
		tx.CreateBucketIfNotExists([]byte("data"))
		return nil
	})
	db.Close()

	table, err = ds.Register[exampleType](exampleType{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		return tx.Add(exampleType{
			Primary: randomString(12),
			Unique:  unique,
		})
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}
}
