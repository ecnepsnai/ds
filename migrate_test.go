package ds

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestMigrate(t *testing.T) {
	type oldType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}
	type newType struct {
		Primary       string `ds:"primary"`
		Index         string `ds:"index"`
		SomethingElse int
	}

	tablePath := path.Join(tmpDir, randomString(12))
	table, err := Register(oldType{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	index := randomString(12)
	for i < count {
		err = table.Add(oldType{
			Primary: randomString(12),
			Index:   index,
			Unique:  randomString(12),
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	table.Close()

	i = 0
	stats := Migrate(MigrateParams{
		TablePath: tablePath,
		OldType:   oldType{},
		NewType:   newType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			old := o.(oldType)
			return newType{
				Primary:       old.Primary,
				Index:         old.Index,
				SomethingElse: i,
			}, nil
		},
	})
	if stats.Error != nil {
		t.Errorf("Error migrating table: %s", stats.Error)
	}
	if !stats.Success {
		t.Error("Migration not successful but error is nil")
	}
	if stats.EntriesMigrated != uint(count) {
		t.Errorf("Not all entries migrated. Expected %d got %d", count, stats.EntriesMigrated)
	}
}

func TestMigrateSkip(t *testing.T) {
	type oldType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}
	type newType struct {
		Primary       string `ds:"primary"`
		Index         string `ds:"index"`
		SomethingElse int
	}

	tablePath := path.Join(tmpDir, randomString(12))
	table, err := Register(oldType{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	index := randomString(12)
	for i < count {
		err = table.Add(oldType{
			Primary: randomString(12),
			Index:   index,
			Unique:  randomString(12),
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	table.Close()

	i = 0
	stats := Migrate(MigrateParams{
		TablePath: tablePath,
		OldType:   oldType{},
		NewType:   newType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			old := o.(oldType)
			if i%2 == 0 {
				i++
				return newType{
					Primary:       old.Primary,
					Index:         old.Index,
					SomethingElse: i,
				}, nil
			}
			i++
			return nil, nil
		},
	})
	if stats.Error != nil {
		t.Errorf("Error migrating table: %s", stats.Error)
	}
	if !stats.Success {
		t.Error("Migration not successful but error is nil")
	}
	expected := uint(count) / 2
	if stats.EntriesMigrated != expected {
		t.Errorf("Unpexpected entry count. Expected %d got %d", count, stats.EntriesMigrated)
	}
	if stats.EntriesSkipped != expected {
		t.Errorf("Unpexpected entry count. Expected %d got %d", count, stats.EntriesSkipped)
	}
}

func TestMigrateFail(t *testing.T) {
	type oldType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}
	type newType struct {
		Primary       string `ds:"primary"`
		Index         string `ds:"index"`
		SomethingElse int
	}

	tablePath := path.Join(tmpDir, randomString(12))
	table, err := Register(oldType{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 100
	index := randomString(12)
	for i < count {
		err = table.Add(oldType{
			Primary: randomString(12),
			Index:   index,
			Unique:  randomString(12),
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		i++
	}

	table.Close()

	i = 0
	stats := Migrate(MigrateParams{
		TablePath: tablePath,
		OldType:   oldType{},
		NewType:   newType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			old := o.(oldType)
			if i == count/2 {
				return nil, fmt.Errorf("Fake error")
			}
			i++
			return newType{
				Primary:       old.Primary,
				Index:         old.Index,
				SomethingElse: i,
			}, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seens for failed migration")
	}
	if stats.Success {
		t.Error("Migration successful but migration failed")
	}
}

func TestMigrateParams(t *testing.T) {
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	tablePath := path.Join(tmpDir, randomString(12))
	table, err := Register(exampleType{}, tablePath, nil)
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
	table.Close()

	stats := Migrate(MigrateParams{
		OldType: exampleType{},
		NewType: exampleType{},
		NewPath: tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	stats = Migrate(MigrateParams{
		TablePath: tablePath,
		NewType:   exampleType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	stats = Migrate(MigrateParams{
		TablePath: tablePath,
		OldType:   exampleType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	stats = Migrate(MigrateParams{
		TablePath: tablePath,
		OldType:   exampleType{},
		NewType:   exampleType{},
		MigrateObject: func(o interface{}) (interface{}, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	stats = Migrate(MigrateParams{
		TablePath: tablePath,
		NewPath:   tablePath,
		OldType:   exampleType{},
		NewType:   exampleType{},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	ioutil.WriteFile(tablePath+"_backup", []byte(""), os.ModePerm)
	stats = Migrate(MigrateParams{
		TablePath: tablePath,
		OldType:   exampleType{},
		NewType:   exampleType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}
}
