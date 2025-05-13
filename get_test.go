package ds_test

import (
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
	"go.etcd.io/bbolt"
)

func TestGet(t *testing.T) {
	t.Parallel()

	primaryKey := randomString(12)
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
		err = tx.Add(exampleType{
			Primary: primaryKey,
			Index:   randomString(12),
			Unique:  randomString(12),
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		got, err := tx.Get(primaryKey)
		if err != nil {
			t.Errorf("Error getting object: %s", err.Error())
		}
		if got.Primary != primaryKey {
			t.Errorf("Incorrect primary key returned. Expected '%s' got '%s", primaryKey, got)
		}
		return nil
	})
}

func TestGetIndex(t *testing.T) {
	t.Parallel()

	index := randomString(12)
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
		i := 0
		count := 10
		for i < count {
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

		objects, err := tx.GetIndex("Index", index, nil)
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) != count {
			t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
		}
		return nil
	})
}

// Test that if there are unmatched index values they aren't returned
func TestGetUnmatchedIndex(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	tablePath := path.Join(t.TempDir(), randomString(12))
	table, err := ds.Register[exampleType](tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		// Add data to the table
		i := 0
		count := 10
		for i < count {
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
		return nil
	})
	table.Close()

	// Delete just the data buckets - leaving everything else
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

	table, err = ds.Register[exampleType](tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartRead(func(tx ds.IReadTransaction[exampleType]) error {
		// This should return nothing
		objects, err := tx.GetIndex("Index", index, nil)
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) > 0 {
			t.Errorf("Unexpected object count returned. Expected 0 got %d", len(objects))
		}
		return nil
	})
}

func TestGetIndexSortedAscending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		// Ascending
		objects, err := tx.GetIndex("Index", index, &ds.GetOptions{Sorted: true, Ascending: true})
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) != count {
			t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
		}
		for i, example := range objects {
			expect := (count - 1) - i
			if expect != example.Value {
				t.Errorf("Unexpected sorted object value. Expected %d got %d", expect, example.Value)
			}
		}
		return nil
	})
}

func TestGetIndexSortedDescending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		// Descending
		objects, err := tx.GetIndex("Index", index, &ds.GetOptions{Sorted: true, Ascending: false})
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) != count {
			t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
		}
		for i, example := range objects {
			if i != example.Value {
				t.Errorf("Unexpected sorted object value. Expected %d got %d", i, example.Value)
			}
		}
		return nil
	})
}

func TestGetIndexSortedNonSortedTable(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), &ds.Options{DisableSorting: true})
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		// Ascending
		objects, err := tx.GetIndex("Index", index, &ds.GetOptions{Sorted: true, Ascending: true})
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) != count {
			t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
		}
		return nil
	})
}

func TestGetUnique(t *testing.T) {
	t.Parallel()

	unique := randomString(12)
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
		err = tx.Add(exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  unique,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		got, err := tx.GetUnique("Unique", unique)
		if err != nil {
			t.Errorf("Error getting object: %s", err.Error())
		}
		if got == nil {
			t.Fatalf("No data returned when expected")
		}
		if got.Unique != unique {
			t.Errorf("Incorrect unique value returned. Expected '%s' got '%s", unique, got)
		}
		return nil
	})
}

func TestGetUnmatchedUnique(t *testing.T) {
	t.Parallel()

	unique := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Unique  string `ds:"unique"`
	}

	tablePath := path.Join(t.TempDir(), randomString(12))
	table, err := ds.Register[exampleType](tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		err = tx.Add(exampleType{
			Primary: randomString(12),
			Unique:  unique,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		return nil
	})
	table.Close()

	// Delete just the data buckets - leaving everything else
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

	table, err = ds.Register[exampleType](tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartRead(func(tx ds.IReadTransaction[exampleType]) error {
		v, err := tx.GetUnique("Unique", unique)
		if err != nil {
			t.Errorf("Error getting object: %s", err.Error())
		}
		if v != nil {
			t.Errorf("Unexpected data returned when none expected")
		}
		return nil
	})
}

func TestGetNilPrimaryKey(t *testing.T) {
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

	table.StartRead(func(tx ds.IReadTransaction[exampleType]) error {
		object, err := tx.Get(nil)
		if err != nil {
			t.Errorf("Unexpected error getting value: %s", err.Error())
		}
		if object != nil {
			t.Errorf("Unexpected object value returned for nil primary key")
		}
		return nil
	})
}

func TestGetNonindexedField(t *testing.T) {
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

	table.StartRead(func(tx ds.IReadTransaction[exampleType]) error {
		object, err := tx.GetIndex(randomString(12), randomString(12), nil)
		if err == nil {
			t.Errorf("No error seen while attempting to get nonindexed field")
		}
		if object != nil {
			t.Errorf("Unexpected object value returned for nil primary key")
		}
		return nil
	})
}

func TestGetNonuniqueField(t *testing.T) {
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

	table.StartRead(func(tx ds.IReadTransaction[exampleType]) error {
		object, err := tx.GetUnique(randomString(12), randomString(12))
		if err == nil {
			t.Errorf("No error seen while attempting to get nonunique field")
		}
		if object != nil {
			t.Errorf("Unexpected object value returned for nil primary key")
		}
		return nil
	})
}

func TestGetAll(t *testing.T) {
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
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   randomString(12),
				Unique:  randomString(12),
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		objects, err := tx.GetAll(nil)
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) != count {
			t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
		}
		return nil
	})
}

func TestGetAllSortedAscending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		// Ascending
		objects, err := tx.GetAll(&ds.GetOptions{Sorted: true, Ascending: true})
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) != count {
			t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
		}
		for i, example := range objects {
			expect := (count - 1) - i
			if expect != example.Value {
				t.Errorf("Unexpected sorted object value. Expected %d got %d", expect, example.Value)
			}
		}
		return nil
	})
}

func TestGetAllSortedDescending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		// Descending
		objects, err := tx.GetAll(&ds.GetOptions{Sorted: true, Ascending: false})
		if err != nil {
			t.Errorf("Error getting many objects: %s", err.Error())
		}
		if len(objects) != count {
			t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
		}
		for i, example := range objects {
			if i != example.Value {
				t.Errorf("Unexpected sorted object value. Expected %d got %d", i, example.Value)
			}
		}
		return nil
	})
}

func TestGetNoResults(t *testing.T) {
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

	table.StartRead(func(tx ds.IReadTransaction[exampleType]) error {
		object, err := tx.Get(randomString(12))
		if err != nil {
			t.Errorf("Unexpected error getting item: %s", err.Error())
		}
		if object != nil {
			t.Error("Object(s) returned when expected nil")
		}

		objects, err := tx.GetIndex("Index", randomString(12), nil)
		if err != nil {
			t.Errorf("Unexpected error getting item: %s", err.Error())
		}
		if len(objects) > 0 {
			t.Error("Object(s) returned when expected nil")
		}

		object, err = tx.GetUnique("Unique", randomString(12))
		if err != nil {
			t.Errorf("Unexpected error getting item: %s", err.Error())
		}
		if object != nil {
			t.Error("Object(s) returned when expected nil")
		}

		objects, err = tx.GetAll(&ds.GetOptions{Sorted: true})
		if err != nil {
			t.Errorf("Unexpected error getting item: %s", err.Error())
		}
		if len(objects) > 0 {
			t.Errorf("Object(s) returned when expected nil: %+v", objects)
		}
		return nil
	})
}

func TestGetIndexMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		max := 5
		objects, err := tx.GetIndex("Index", index, &ds.GetOptions{Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned := len(objects)
		if returned != max {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
		}

		tx.DeleteAll()
		err = tx.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   0,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		objects, err = tx.GetIndex("Index", index, &ds.GetOptions{Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned = len(objects)
		if returned != 1 {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
		}
		return nil
	})
}

func TestGetAllMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		max := 5
		objects, err := tx.GetAll(&ds.GetOptions{Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned := len(objects)
		if returned != max {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
		}

		tx.DeleteAll()
		err = tx.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   0,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		objects, err = tx.GetAll(&ds.GetOptions{Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned = len(objects)
		if returned != 1 {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
		}
		return nil
	})
}

func TestGetIndexSortedMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		max := 5
		objects, err := tx.GetIndex("Index", index, &ds.GetOptions{Sorted: true, Ascending: true, Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned := len(objects)
		if returned != max {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
		}

		tx.DeleteAll()
		err = tx.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   0,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		objects, err = tx.GetIndex("Index", index, &ds.GetOptions{Sorted: true, Ascending: true, Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned = len(objects)
		if returned != 1 {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
		}
		return nil
	})
}

func TestGetAllSortedMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := ds.Register[exampleType](path.Join(t.TempDir(), randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction[exampleType]) error {
		i := 0
		count := 10
		for i < count {
			err = tx.Add(exampleType{
				Primary: randomString(12),
				Index:   index,
				Value:   i,
			})
			if err != nil {
				t.Errorf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		max := 5
		objects, err := tx.GetAll(&ds.GetOptions{Sorted: true, Ascending: true, Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned := len(objects)
		if returned != max {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
		}

		tx.DeleteAll()
		err = tx.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   0,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}

		objects, err = tx.GetAll(&ds.GetOptions{Sorted: true, Ascending: true, Max: max})
		if err != nil {
			t.Errorf("Error getting many entires: %s", err.Error())
		}

		returned = len(objects)
		if returned != 1 {
			t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
		}
		return nil
	})
}
