package ds

import (
	"path"
	"testing"
)

func TestGet(t *testing.T) {
	t.Parallel()

	primaryKey := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
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

	v, err := table.Get(primaryKey)
	if err != nil {
		t.Errorf("Error getting object: %s", err.Error())
	}
	got := v.(exampleType).Primary
	if got != primaryKey {
		t.Errorf("Incorrect primary key returned. Expected '%s' got '%s", primaryKey, got)
	}
}

func TestGetIndex(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
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

	objects, err := table.GetIndex("Index", index, nil)
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
}

func TestGetIndexSortedAscending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
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
	objects, err := table.GetIndex("Index", index, &GetOptions{Sorted: true, Ascending: true})
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
	for i, object := range objects {
		example := object.(exampleType)
		expect := (count - 1) - i
		if expect != example.Value {
			t.Errorf("Unexpected sorted object value. Expected %d got %d", expect, example.Value)
		}
	}
}

func TestGetIndexSortedDescending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
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
	objects, err := table.GetIndex("Index", index, &GetOptions{Sorted: true, Ascending: false})
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
	for i, object := range objects {
		example := object.(exampleType)
		if i != example.Value {
			t.Errorf("Unexpected sorted object value. Expected %d got %d", i, example.Value)
		}
	}
}

func TestGetIndexSortedNonSortedTable(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), &Options{DisableSorting: true})
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
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
	objects, err := table.GetIndex("Index", index, &GetOptions{Sorted: true, Ascending: true})
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
}

func TestGetUnique(t *testing.T) {
	t.Parallel()

	unique := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
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

	v, err := table.GetUnique("Unique", unique)
	if err != nil {
		t.Errorf("Error getting object: %s", err.Error())
	}
	got := v.(exampleType).Unique
	if got != unique {
		t.Errorf("Incorrect unique value returned. Expected '%s' got '%s", unique, got)
	}
}

func TestGetNilPrimaryKey(t *testing.T) {
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

	object, err := table.Get(nil)
	if err != nil {
		t.Errorf("Unexpected error getting value: %s", err.Error())
	}
	if object != nil {
		t.Errorf("Unexpected object value returned for nil primary key")
	}
}

func TestGetNonindexedField(t *testing.T) {
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

	object, err := table.GetIndex(randomString(12), randomString(12), nil)
	if err == nil {
		t.Errorf("No error seen while attempting to get nonindexed field")
	}
	if object != nil {
		t.Errorf("Unexpected object value returned for nil primary key")
	}
}

func TestGetNonuniqueField(t *testing.T) {
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

	object, err := table.GetUnique(randomString(12), randomString(12))
	if err == nil {
		t.Errorf("No error seen while attempting to get nonunique field")
	}
	if object != nil {
		t.Errorf("Unexpected object value returned for nil primary key")
	}
}

func TestGetAll(t *testing.T) {
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

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
			Primary: randomString(12),
			Index:   randomString(12),
			Unique:  randomString(12),
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	objects, err := table.GetAll(nil)
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
}

func TestGetAllSortedAscending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
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
	objects, err := table.GetAll(&GetOptions{Sorted: true, Ascending: true})
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
	for i, object := range objects {
		example := object.(exampleType)
		expect := (count - 1) - i
		if expect != example.Value {
			t.Errorf("Unexpected sorted object value. Expected %d got %d", expect, example.Value)
		}
	}
}

func TestGetAllSortedDescending(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
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
	objects, err := table.GetAll(&GetOptions{Sorted: true, Ascending: false})
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
	for i, object := range objects {
		example := object.(exampleType)
		if i != example.Value {
			t.Errorf("Unexpected sorted object value. Expected %d got %d", i, example.Value)
		}
	}
}

func TestGetNoResults(t *testing.T) {
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

	object, err := table.Get(randomString(12))
	if err != nil {
		t.Errorf("Unexpected error getting item: %s", err.Error())
	}
	if object != nil {
		t.Error("Object(s) returned when expected nil")
	}

	objects, err := table.GetIndex("Index", randomString(12), nil)
	if err != nil {
		t.Errorf("Unexpected error getting item: %s", err.Error())
	}
	if len(objects) > 0 {
		t.Error("Object(s) returned when expected nil")
	}

	object, err = table.GetUnique("Unique", randomString(12))
	if err != nil {
		t.Errorf("Unexpected error getting item: %s", err.Error())
	}
	if object != nil {
		t.Error("Object(s) returned when expected nil")
	}

	objects, err = table.GetAll(&GetOptions{Sorted: true})
	if err != nil {
		t.Errorf("Unexpected error getting item: %s", err.Error())
	}
	if object != nil {
		t.Error("Object(s) returned when expected nil")
	}
}

func TestGetIndexMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   i,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	max := 20
	objects, err := table.GetIndex("Index", index, &GetOptions{Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned := len(objects)
	if returned != max {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
	}

	table.DeleteAll()
	err = table.Add(exampleType{
		Primary: randomString(12),
		Index:   index,
		Value:   0,
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	objects, err = table.GetIndex("Index", index, &GetOptions{Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned = len(objects)
	if returned != 1 {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
	}
}

func TestGetAllMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   i,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	max := 20
	objects, err := table.GetAll(&GetOptions{Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned := len(objects)
	if returned != max {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
	}

	table.DeleteAll()
	err = table.Add(exampleType{
		Primary: randomString(12),
		Index:   index,
		Value:   0,
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	objects, err = table.GetAll(&GetOptions{Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned = len(objects)
	if returned != 1 {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
	}
}

func TestGetIndexSortedMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   i,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	max := 20
	objects, err := table.GetIndex("Index", index, &GetOptions{Sorted: true, Ascending: true, Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned := len(objects)
	if returned != max {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
	}

	table.DeleteAll()
	err = table.Add(exampleType{
		Primary: randomString(12),
		Index:   index,
		Value:   0,
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	objects, err = table.GetIndex("Index", index, &GetOptions{Sorted: true, Ascending: true, Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned = len(objects)
	if returned != 1 {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
	}
}

func TestGetAllSortedMaximum(t *testing.T) {
	t.Parallel()

	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Value   int    `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)), nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	for i < count {
		err = table.Add(exampleType{
			Primary: randomString(12),
			Index:   index,
			Value:   i,
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	max := 20
	objects, err := table.GetAll(&GetOptions{Sorted: true, Ascending: true, Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned := len(objects)
	if returned != max {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", max, returned)
	}

	table.DeleteAll()
	err = table.Add(exampleType{
		Primary: randomString(12),
		Index:   index,
		Value:   0,
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	objects, err = table.GetAll(&GetOptions{Sorted: true, Ascending: true, Max: max})
	if err != nil {
		t.Errorf("Error getting many entires: %s", err.Error())
	}

	returned = len(objects)
	if returned != 1 {
		t.Errorf("Returned number of entries was not correct. Expected %d got %d", 1, returned)
	}
}
