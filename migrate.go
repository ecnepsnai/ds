package ds

import (
	"fmt"
	"os"
	"reflect"

	"github.com/ecnepsnai/logtic"
)

// MigrateParams describes the parameters to perform a DS table migration.
// All fields are required unless otherwise specified.
//
//   - `OldType` is an instance of a struct object that has the same definition as the existing table.
//   - `NewType` is an instance of a struct object that has the definition that shall be used. This can be the same as
//     the OldType.
type MigrateParams[OldType any, NewType any] struct {
	// TablePath the path to the existing table file
	TablePath string
	// NewPath (optional) is the path for the new table file. If blank then the existing table path is re-used.
	NewPath string
	// DisableSorting (optional) if the current table is sorted, set this to true to disable sorting
	// Note: This is irreversible!
	DisableSorting bool
	// MigrateObject method called for each entry in the table in reverse order. Return a new type, error, or nil.
	// Migration is halted if an error is returned.
	// Return (nil, nil) and the entry will be skipped from migration, but migration will continue.
	MigrateObject func(o *OldType) (*NewType, error)
	// KeepBackup (optional) if false the backup copy of the table will be discarded if the migration was successful. If
	// true, the copy is not deleted.
	KeepBackup bool
}

func (params *MigrateParams[OldType, NewType]) validate() error {
	var oldT OldType
	var newT NewType

	if _, err := os.Stat(params.TablePath); err != nil {
		return fmt.Errorf("%s: %s", ErrMigrateTablePathNotFound, err.Error())
	}
	if params.NewPath == "" {
		params.NewPath = params.TablePath
	}
	if params.MigrateObject == nil {
		return fmt.Errorf("%s: MigrateObject", ErrMissingRequiredValue)
	}
	if typeOf := reflect.TypeOf(oldT); typeOf.Kind() == reflect.Ptr {
		return fmt.Errorf("%s: NewType", ErrPointer)
	}
	if typeOf := reflect.TypeOf(newT); typeOf.Kind() == reflect.Ptr {
		return fmt.Errorf("%s: OldType", ErrPointer)
	}

	return nil
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

// Migrate will migrate a DS table from one object type to another. You must migrate if the old data type is not
// compatible with the new type, such as if an existing field was changed. You don't need to migrate if you add a new
// field or remove an existing field.
//
// Before the existing data is touched, a copy is made with "_backup" appended to the filename, and a new table file is
// created with the migrated entries. Upon successful migration, the backup copy is deleted by default. If the table
// being migrated is sorted, the original order is preserved.
//
// Ensure you read the documentation of the MigrateParams struct, as it goes into greater detail on the parameters
// required for migration, and what they do.
func Migrate[OldType any, NewType any](params MigrateParams[OldType, NewType]) (results MigrationResults) {
	log := logtic.Log.Connect("ds-migration")

	if err := params.validate(); err != nil {
		log.Error("%s", err.Error())
		results.Success = false
		results.Error = err
		return
	}

	backupPath := params.TablePath + "_backup"
	if _, err := os.Stat(backupPath); err == nil {
		log.Error("Backup copy of table already exists at '%s'", backupPath)
		results.Success = false
		results.Error = os.ErrExist
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

	oldTable, err := Register[OldType](backupPath, options)
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

	table, err := Register[NewType](params.NewPath, options)
	if err != nil {
		log.Error("Error registering new table: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}
	defer table.Close()

	objects, err := oldTable.getAll(&GetOptions{Sorted: true, Ascending: true})
	if err != nil {
		log.Error("Error getting all entires: %s", err.Error())
		results.Success = false
		results.Error = err
		return
	}

	i := len(objects) - 1
	for i >= 0 {
		object := objects[i]
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
			i--
			continue
		}
		if err := table.add(*newObject); err != nil {
			log.Error("Error adding new entry to table: %s", err.Error())
			results.Success = false
			results.Error = err
			return
		}
		log.Debug("Migrating entry at index %d", i)
		results.EntriesMigrated++
		i--
	}

	log.PInfo("Migration successful", map[string]any{
		"table_path":     params.TablePath,
		"migrated_count": results.EntriesMigrated,
		"skipped_count":  results.EntriesSkipped,
	})
	results.Success = true

	if !params.KeepBackup {
		os.Remove(backupPath)
		log.Debug("Removed backup copy '%s'", backupPath)
	}

	return
}
