package ds

import (
	"fmt"
	"os"
	"reflect"

	"github.com/ecnepsnai/logtic"
)

// MigrateParams describes the parameters to perform a DS table migration.
// All fields are required.
type MigrateParams struct {
	// TablePath the path to the existing table
	TablePath string
	// NewPath the path for the new table. This can be the same as the old table.
	NewPath string
	// OldType the old type (current type of the table)
	OldType interface{}
	// NewType the new type. This can be the same as the OldType.
	NewType interface{}
	// DisableSorting if the current table is sorted, set this to true to disable sorting
	// Note: This is irreversible!
	DisableSorting bool
	// MigrateObject method called for each entry in the table. Return a new type or error.
	// migration is halted if an error is returned.
	// Return (nil, nil) and the entry will be skipped from migration.
	MigrateObject func(o interface{}) (interface{}, error)
}

// MigrationResults describes results from a migration
type MigrationResults struct {
	// Success was the migration successful
	Success bool
	// Error if unsuccessful, this will be the error that caused the failure
	Error error
	// EntriesMigrated the number of entries migrated
	EntriesMigrated uint
	// EntriesSkipped the number of entries skipped
	EntriesSkipped uint
}

// Migrate will migrate a DS table from one object type to another.
// The migration process appends "_backup" to the current tables filename and
// does not update it in any way. A new table file is created with the migrated entries
// and indexes.
func Migrate(params MigrateParams) (results MigrationResults) {
	log := logtic.Connect("ds-migration")

	if _, err := os.Stat(params.TablePath); err != nil {
		log.Error("TablePath does not exist or cannot be accessed: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}
	if params.NewPath == "" {
		log.Error("NewPath is required")
		results.Success = false
		results.Error = fmt.Errorf("newPath required")
		return
	}
	if params.OldType == nil {
		log.Error("OldType is required")
		results.Success = false
		results.Error = fmt.Errorf("oldType required")
		return
	}
	if params.NewType == nil {
		log.Error("NewType is required")
		results.Success = false
		results.Error = fmt.Errorf("newType required")
		return
	}
	if params.MigrateObject == nil {
		log.Error("MigrateObject method required")
		results.Success = false
		results.Error = fmt.Errorf("migrateObject method required")
		return
	}
	if typeOf := reflect.TypeOf(params.NewType); typeOf.Kind() == reflect.Ptr {
		log.Error("NewType cannot be a pointer")
		results.Success = false
		results.Error = fmt.Errorf("newType cannot be a pointer")
		return
	}
	if typeOf := reflect.TypeOf(params.OldType); typeOf.Kind() == reflect.Ptr {
		log.Error("OldType cannot be a pointer")
		results.Success = false
		results.Error = fmt.Errorf("oldType cannot be a pointer")
		return
	}

	backupPath := params.TablePath + "_backup"
	if _, err := os.Stat(backupPath); err == nil {
		log.Error("Backup copy of table already exists at '%s'", backupPath)
		results.Success = false
		results.Error = fmt.Errorf("backup copy of table exists")
		return
	}

	if err := os.Rename(params.TablePath, backupPath); err != nil {
		log.Error("Failed to rename existing table: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}

	options, err := getTableOptions(backupPath)
	if err != nil {
		log.Error("Error getting table options: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}
	options.force = true

	oldTable, err := Register(params.OldType, backupPath, options)
	if err != nil {
		log.Error("Error registering old table: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}
	defer oldTable.Close()

	if params.DisableSorting && !oldTable.options.DisableSorting {
		options.DisableSorting = true
	}

	table, err := Register(params.NewType, params.NewPath, options)
	if err != nil {
		log.Error("Error registering new table: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}
	defer table.Close()

	objects, err := oldTable.GetAll(&GetOptions{Sorted: true, Ascending: true})
	if err != nil {
		log.Error("Error getting all entires: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}

	for i, object := range objects {
		newObject, err := params.MigrateObject(object)
		if err != nil {
			log.Error("Object migration failed - aborting migration")
			results.Success = false
			results.Error = err
			return
		}
		if newObject == nil {
			log.Debug("Skipping entry at index %d", i)
			results.EntriesSkipped++
			continue
		}
		if err := table.Add(newObject); err != nil {
			log.Error("Error adding new entry to table: %s", err.Error())
			results.Success = false
			results.Error = err
			return
		}
		log.Debug("Migrating entry at index %d", i)
		results.EntriesMigrated++
	}

	log.Info("Migration successful")
	results.Success = true
	return
}
