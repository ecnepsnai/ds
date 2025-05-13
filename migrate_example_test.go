package ds_test

import (
	"fmt"

	"github.com/ecnepsnai/ds"
)

func ExampleMigrate() {
	// Define a struct that maps to the current type used in the table
	type oldType struct {
		Username  string `ds:"primary"`
		Email     string `ds:"unique"`
		FirstName string
		LastName  string
	}

	// Define your new struct
	type newType struct {
		Username string `ds:"primary"`
		Email    string `ds:"unique"`
		Name     string
	}

	// In this example, we're merging the "FirstName" and "LastName" fields of the User object to
	// just a single "Name" field
	// NewType can be the same as the old type if you aren't changing the struct
	result := ds.Migrate(ds.MigrateParams[oldType, newType]{
		TablePath: "/path/to/table.db",
		NewPath:   "/path/to/table.db", // You can specify the same path, or a new one if you want
		MigrateObject: func(old *oldType) (*newType, error) {
			// Within the MigrateObject function you can:
			// 1. Return a object of the NewType (specified in the MigrateParams)
			// 2. Return an error and the migration will abort
			// 3. Return nil and this entry will be skipped
			return &newType{
				Username: old.Username,
				Email:    old.Email,
				Name:     old.FirstName + " " + old.LastName,
			}, nil
		},
	})
	if !result.Success {
		// Migration failed.
		panic(result.Error)
	}

	fmt.Printf("Migration successful. Entries migrated: %d, skipped: %d\n", result.EntriesMigrated, result.EntriesSkipped)
}
