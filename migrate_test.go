package ds_test

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
)

// Test that a migration succeeded
func TestMigrate(t *testing.T) {
	t.Parallel()

	count := 10

	registerTable := func() string {
		type user struct {
			Username string `ds:"primary"`
			Email    string `ds:"unique"`
			Enabled  bool   `ds:"index"`
			Password string
		}

		tp := path.Join(tmpDir, randomString(12))
		table, err := ds.Register(user{}, tp, nil)
		if err != nil {
			t.Fatalf("Error registering table: %s", err.Error())
		}

		i := 0
		for i < count {
			err = table.Add(user{
				Username: randomString(24),
				Email:    randomString(24),
				Enabled:  true,
				Password: randomString(24),
			})
			if err != nil {
				t.Fatalf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		table.Close()
		return tp
	}

	tablePath := registerTable()

	type oldUser struct {
		Username string `ds:"primary"`
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
		Password string
	}
	type newUser struct {
		ID       string `ds:"primary"`
		Username string `ds:"unique"`
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
		Password string
	}

	stats := ds.Migrate(ds.MigrateParams{
		TablePath: tablePath,
		OldType:   oldUser{},
		NewType:   newUser{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			old := o.(oldUser)
			return newUser{
				ID:       randomString(24),
				Username: old.Username,
				Email:    old.Email,
				Enabled:  old.Enabled,
				Password: old.Password,
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

// Test that entries can be skipped in a migration
func TestMigrateSkip(t *testing.T) {
	t.Parallel()

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
	table, err := ds.Register(oldType{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 10
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
	stats := ds.Migrate(ds.MigrateParams{
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
		t.Errorf("Unexpected entry count. Expected %d got %d", count, stats.EntriesMigrated)
	}
	if stats.EntriesSkipped != expected {
		t.Errorf("Unexpected entry count. Expected %d got %d", count, stats.EntriesSkipped)
	}
}

// Test that a migration will fail if an error is returned
func TestMigrateFail(t *testing.T) {
	t.Parallel()

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
	table, err := ds.Register(oldType{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 10
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
	stats := ds.Migrate(ds.MigrateParams{
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
		t.Errorf("No error seen for failed migration")
	}
	if stats.Success {
		t.Error("Migration successful but migration failed")
	}
}

// Test that the all required parameters are present when requesting a migration
func TestMigrateParams(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	tablePath := path.Join(tmpDir, randomString(12))
	table, err := ds.Register(exampleType{}, tablePath, nil)
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

	// Missing table path
	stats := ds.Migrate(ds.MigrateParams{
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

	// Missing old type
	stats = ds.Migrate(ds.MigrateParams{
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

	// Missing new type
	stats = ds.Migrate(ds.MigrateParams{
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

	// New type is pointer
	stats = ds.Migrate(ds.MigrateParams{
		TablePath: tablePath,
		NewType:   &exampleType{},
		OldType:   exampleType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Old type is pointer
	stats = ds.Migrate(ds.MigrateParams{
		TablePath: tablePath,
		NewType:   exampleType{},
		OldType:   &exampleType{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Missing new path
	stats = ds.Migrate(ds.MigrateParams{
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

	// Missing migrate method
	stats = ds.Migrate(ds.MigrateParams{
		TablePath: tablePath,
		NewPath:   tablePath,
		OldType:   exampleType{},
		NewType:   exampleType{},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Backup already exists
	os.WriteFile(tablePath+"_backup", []byte(""), os.ModePerm)
	stats = ds.Migrate(ds.MigrateParams{
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

// Test that migrating a table preserves its original order
func TestMigrateSorted(t *testing.T) {
	t.Parallel()

	tablePath := path.Join(tmpDir, randomString(12))
	type originalUser struct {
		ID     int `ds:"primary"`
		Value1 string
	}

	count := 10

	registerTable := func(tt interface{}) *ds.Table {
		table, err := ds.Register(tt, tablePath, nil)
		if err != nil {
			t.Fatalf("Error registering table: %s", err.Error())
		}

		return table
	}

	registerAndCloseTable := func() {
		table := registerTable(originalUser{})

		i := 0
		for i < count {
			err := table.Add(originalUser{
				ID:     i,
				Value1: randomString(12),
			})
			if err != nil {
				t.Fatalf("Error adding value to table: %s", err.Error())
			}
			i++
		}

		table.Close()
	}

	registerAndCloseTable()

	type newUser struct {
		ID     int `ds:"primary"`
		Value2 string
	}

	stats := ds.Migrate(ds.MigrateParams{
		TablePath: tablePath,
		OldType:   originalUser{},
		NewType:   newUser{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			old := o.(originalUser)
			return newUser{
				ID:     old.ID,
				Value2: old.Value1,
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

	table := registerTable(newUser{})
	defer table.Close()

	objects, err := table.GetAll(&ds.GetOptions{
		Sorted: true,
	})
	if err != nil {
		t.Errorf("Error getting all objects from table: %s", err.Error())
	}
	if len(objects) != count {
		t.Errorf("Incorrect number of objects returned. Expected %d got %d", count, len(objects))
	}
	users := make([]newUser, count)
	for i, obj := range objects {
		users[i] = obj.(newUser)
	}

	for i, user := range users {
		if user.ID != i {
			t.Errorf("Incorrect order of users returned. Expected %d got %d", i, user.ID)
		}
	}
}
