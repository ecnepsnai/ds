package ds

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

// Test that a migration succeeded
func TestMigrate(t *testing.T) {
	count := 100

	registerTable := func() string {
		type user struct {
			Username string `ds:"primary"`
			Email    string `ds:"unique"`
			Enabled  bool   `ds:"index"`
			Password string
		}

		tp := path.Join(tmpDir, randomString(12))
		table, err := Register(user{}, tp, nil)
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
	type user struct {
		ID       string `ds:"primary"`
		Username string `ds:"unique"`
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
		Password string
	}

	stats := Migrate(MigrateParams{
		TablePath: tablePath,
		OldType:   oldUser{},
		NewType:   user{},
		NewPath:   tablePath,
		MigrateObject: func(o interface{}) (interface{}, error) {
			old := o.(oldUser)
			return user{
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

// Test that a migration will fail if an error is returned
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

// Test that the all required parameters are present when requesting a migration
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

	// Missing table path
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

	// Missing old type
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

	// Missing new type
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

	// Missing new path
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

	// Missing migrate method
	stats = Migrate(MigrateParams{
		TablePath: tablePath,
		NewPath:   tablePath,
		OldType:   exampleType{},
		NewType:   exampleType{},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Backup already exists
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
