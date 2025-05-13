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
		type User struct {
			Username string `ds:"primary"`
			Email    string `ds:"unique"`
			Enabled  bool   `ds:"index"`
			Password string
		}

		tp := path.Join(t.TempDir(), randomString(12))
		table, err := ds.Register[User](tp, nil)
		if err != nil {
			t.Fatalf("Error registering table: %s", err.Error())
		}

		err = table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
			i := 0
			for i < count {
				err := tx.Add(User{
					Username: randomString(24),
					Email:    randomString(24),
					Enabled:  true,
					Password: randomString(24),
				})
				if err != nil {
					return err
				}
				i++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Error adding value to table: %s", err.Error())
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

	stats := ds.Migrate(ds.MigrateParams[oldUser, newUser]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *oldUser) (*newUser, error) {
			return &newUser{
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

// Test that a migration where a nested struct type changes succeeds
func TestMigrateStruct(t *testing.T) {
	t.Parallel()

	count := 10

	registerTable := func() string {
		type identification struct {
			Number int
			Expiry string
		}

		type User struct {
			Username       string `ds:"primary"`
			Email          string `ds:"unique"`
			Enabled        bool   `ds:"index"`
			Password       string
			Identification identification
		}

		tp := path.Join(t.TempDir(), randomString(12))
		table, err := ds.Register[User](tp, nil)
		if err != nil {
			t.Fatalf("Error registering table: %s", err.Error())
		}

		err = table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
			i := 0
			for i < count {
				err := tx.Add(User{
					Username: randomString(24),
					Email:    randomString(24),
					Enabled:  true,
					Password: randomString(24),
					Identification: identification{
						Number: i,
						Expiry: randomString(24),
					},
				})
				if err != nil {
					return err
				}
				i++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Error adding value to table: %s", err.Error())
		}

		table.Close()
		return tp
	}

	tablePath := registerTable()

	type oldIdentification struct {
		Number int
		Expiry string
	}
	type oldUser struct {
		Username       string `ds:"primary"`
		Email          string `ds:"unique"`
		Enabled        bool   `ds:"index"`
		Password       string
		Identification oldIdentification
	}
	type newIdentification struct {
		Number string
		Expiry string
	}
	type newUser struct {
		ID             string `ds:"primary"`
		Username       string `ds:"unique"`
		Email          string `ds:"unique"`
		Enabled        bool   `ds:"index"`
		Password       string
		Identification newIdentification
	}

	stats := ds.Migrate[oldUser, newUser](ds.MigrateParams[oldUser, newUser]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *oldUser) (*newUser, error) {
			return &newUser{
				ID:       randomString(24),
				Username: old.Username,
				Email:    old.Email,
				Enabled:  old.Enabled,
				Password: old.Password,
				Identification: newIdentification{
					Number: fmt.Sprintf("%d", old.Identification.Number),
					Expiry: old.Identification.Expiry,
				},
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

	tablePath := path.Join(t.TempDir(), randomString(12))
	table, err := ds.Register[oldType](tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	count := 10
	err = table.StartWrite(func(tx ds.IReadWriteTransaction[oldType]) error {
		i := 0
		index := randomString(12)
		for i < count {
			err = tx.Add(oldType{
				Primary: randomString(12),
				Index:   index,
				Unique:  randomString(12),
			})
			if err != nil {
				return err
			}
			i++
		}
		return nil
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	table.Close()

	i := 0
	stats := ds.Migrate[oldType, newType](ds.MigrateParams[oldType, newType]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *oldType) (*newType, error) {
			if i%2 == 0 {
				i++
				return &newType{
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

	tablePath := path.Join(t.TempDir(), randomString(12))
	table, err := ds.Register[oldType](tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	i := 0
	count := 10
	err = table.StartWrite(func(tx ds.IReadWriteTransaction[oldType]) error {
		index := randomString(12)
		for i < count {
			err = tx.Add(oldType{
				Primary: randomString(12),
				Index:   index,
				Unique:  randomString(12),
			})
			if err != nil {
				return err
			}
			i++
		}
		return nil
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	table.Close()

	i = 0
	stats := ds.Migrate[oldType, newType](ds.MigrateParams[oldType, newType]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *oldType) (*newType, error) {
			if i == count/2 {
				return nil, fmt.Errorf("Fake error")
			}
			i++
			return &newType{
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

	tablePath := path.Join(t.TempDir(), randomString(12))
	table, err := ds.Register[exampleType](tablePath, nil)
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
	table.Close()

	// Missing table path
	stats := ds.Migrate[exampleType, exampleType](ds.MigrateParams[exampleType, exampleType]{
		NewPath: tablePath,
		MigrateObject: func(old *exampleType) (*exampleType, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// New type is pointer
	stats = ds.Migrate[exampleType, exampleType](ds.MigrateParams[exampleType, exampleType]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *exampleType) (*exampleType, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Old type is pointer
	stats = ds.Migrate[exampleType, exampleType](ds.MigrateParams[exampleType, exampleType]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *exampleType) (*exampleType, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Missing new path
	stats = ds.Migrate[exampleType, exampleType](ds.MigrateParams[exampleType, exampleType]{
		TablePath: tablePath,
		MigrateObject: func(old *exampleType) (*exampleType, error) {
			return nil, nil
		},
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Missing migrate method
	stats = ds.Migrate[exampleType, exampleType](ds.MigrateParams[exampleType, exampleType]{
		TablePath: tablePath,
		NewPath:   tablePath,
	})
	if stats.Error == nil {
		t.Errorf("No error seen for invalid migration request")
	}

	// Backup already exists
	os.WriteFile(tablePath+"_backup", []byte(""), os.ModePerm)
	stats = ds.Migrate[exampleType, exampleType](ds.MigrateParams[exampleType, exampleType]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *exampleType) (*exampleType, error) {
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

	tablePath := path.Join(t.TempDir(), randomString(12))
	type originalUser struct {
		ID     int `ds:"primary"`
		Value1 string
	}
	type newUser struct {
		ID     int `ds:"primary"`
		Value2 string
	}

	count := 10

	registerOriginalTable := func(tt any) *ds.Table[originalUser] {
		table, err := ds.Register[originalUser](tablePath, nil)
		if err != nil {
			t.Fatalf("Error registering table: %s", err.Error())
		}

		return table
	}

	registerNewTable := func(tt any) *ds.Table[newUser] {
		table, err := ds.Register[newUser](tablePath, nil)
		if err != nil {
			t.Fatalf("Error registering table: %s", err.Error())
		}

		return table
	}

	registerAndCloseTable := func() {
		table := registerOriginalTable(originalUser{})

		i := 0
		err := table.StartWrite(func(tx ds.IReadWriteTransaction[originalUser]) error {
			for i < count {
				err := tx.Add(originalUser{
					ID:     i,
					Value1: randomString(12),
				})
				if err != nil {
					return err
				}
				i++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Error adding value to table: %s", err.Error())
		}

		table.Close()
	}

	registerAndCloseTable()

	stats := ds.Migrate(ds.MigrateParams[originalUser, newUser]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *originalUser) (*newUser, error) {
			return &newUser{
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

	table := registerNewTable(newUser{})
	defer table.Close()

	var users []*newUser
	var err error
	err = table.StartRead(func(tx ds.IReadTransaction[newUser]) error {
		users, err = tx.GetAll(&ds.GetOptions{
			Sorted: true,
		})
		return err
	})
	if err != nil {
		t.Errorf("Error getting all objects from table: %s", err.Error())
	}
	if len(users) != count {
		t.Errorf("Incorrect number of objects returned. Expected %d got %d", count, len(users))
	}

	for i, user := range users {
		if user.ID != i {
			t.Errorf("Incorrect order of users returned. Expected %d got %d", i, user.ID)
		}
	}
}

// Test that a migration suceeds when we add an indexed field
func TestMigrateAddIndex(t *testing.T) {
	t.Parallel()

	count := 10

	registerTable := func() string {
		type User struct {
			Username string `ds:"primary" json:"username"`
			Email    string `json:"email"`
			Enabled  bool   `ds:"index" json:"enabled"`
			Password string `json:"-"`
		}

		tp := path.Join(t.TempDir(), randomString(12))
		table, err := ds.Register[User](tp, nil)
		if err != nil {
			t.Fatalf("Error registering table: %s", err.Error())
		}

		err = table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
			i := 0
			for i < count {
				err := tx.Add(User{
					Username: randomString(24),
					Email:    randomString(24),
					Enabled:  true,
					Password: randomString(24),
				})
				if err != nil {
					return err
				}
				i++
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Error adding value to table: %s", err.Error())
		}

		table.Close()
		return tp
	}

	tablePath := registerTable()

	type oldUser struct {
		Username string `ds:"primary" json:"username"`
		Email    string `json:"email"`
		Enabled  bool   `ds:"index" json:"enabled"`
		Password string `json:"-"`
	}
	type newUser struct {
		Username string `ds:"primary" json:"username"`
		Email    string `ds:"unique" json:"email"`
		Enabled  bool   `ds:"index" json:"enabled"`
		Password string `json:"-"`
	}

	stats := ds.Migrate(ds.MigrateParams[oldUser, newUser]{
		TablePath: tablePath,
		NewPath:   tablePath,
		MigrateObject: func(old *oldUser) (*newUser, error) {
			return &newUser{
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
