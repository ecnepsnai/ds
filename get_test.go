package ds

import (
	"path"
	"testing"
)

func TestGet(t *testing.T) {
	primaryKey := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)))
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
	index := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)))
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

	objects, err := table.GetIndex("Index", index)
	if err != nil {
		t.Errorf("Error getting many objects: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Unexpected object count returned. Expected %d got %d", count, len(objects))
	}
}

func TestGetUnique(t *testing.T) {
	unique := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)))
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
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)))
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
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)))
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	object, err := table.GetIndex(randomString(12), randomString(12))
	if err == nil {
		t.Errorf("No error seen while attempting to get nonindexed field")
	}
	if object != nil {
		t.Errorf("Unexpected object value returned for nil primary key")
	}
}

func TestGetNonuniqueField(t *testing.T) {
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)))
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
