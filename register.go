package ds

import (
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/ecnepsnai/logtic"
	"go.etcd.io/bbolt"
)

// The current version of the DS schema. This is not currently used but is reserved for future
// use when we may need to change how we store data in tables from older versions of DS.
var currentDSSchemaVersion = 1

// Register will register an instance of a struct with ds, creating a table (or opening an existing table) for this type
// at the specified file path.
func Register[T any](filePath string, options *Options) (*Table[T], error) {
	var o T
	typeOf := reflect.TypeOf(o)

	// Gob will panic if you register the same object twice.
	// I think this is stupid, so we will recover from this if
	// registering the type panics.
	registerGobType(o)
	registerGobType(Options{})
	registerGobType(Config{})

	table := Table[T]{
		Name:   typeOf.Name(),
		typeOf: typeOf,
		log:    logtic.Log.Connect("ds(" + typeOf.Name() + ")"),
		txLock: &sync.RWMutex{},
	}
	if options != nil {
		table.options = *options
	}

	primaryKey, indexes, uniques, err := table.getKeysFromFields()
	if err != nil {
		return nil, err
	}

	table.primaryKey = primaryKey
	table.indexes = indexes
	table.uniques = uniques

	if err := checkForExistingBoltTable(filePath); err != nil {
		table.log.Error("Existing file '%s' that is not recognized as a DS table", filePath)
		return nil, fmt.Errorf("%s: %s", ErrBadTableFile, err.Error())
	}

	force := false
	if options != nil {
		force = options.force
	}
	if err := table.createTableBuckets(filePath, force); err != nil {
		return nil, err
	}

	table.log.Info("Datastore '%s' opened at '%s'", table.Name, filePath)
	return &table, nil
}

func (table *Table[T]) createTableBuckets(filePath string, force bool) error {
	data, err := bbolt.Open(filePath, os.ModePerm, nil)
	if err != nil {
		table.log.Error("Error opening bolt database: %s", err.Error())
		return err
	}
	table.data = data

	err = data.Update(func(tx *bbolt.Tx) error {
		if err := table.initializeConfig(tx, force); err != nil {
			table.log.Error("Error initializing config: %s", err.Error())
			return err
		}

		if _, err = tx.CreateBucketIfNotExists(dataKey); err != nil {
			table.log.Error("Error creating bucket '%s: %s", "data", err.Error())
			return err
		}
		if !table.options.DisableSorting {
			if _, err = tx.CreateBucketIfNotExists(insertOrderKey); err != nil {
				table.log.Error("Error creating bucket '%s: %s", insertOrderKey, err.Error())
				return err
			}
		}
		for _, index := range table.indexes {
			if _, err = tx.CreateBucketIfNotExists([]byte(indexPrefix + index)); err != nil {
				table.log.Error("Error creating bucket '%s: %s", indexPrefix+index, err.Error())
				return err
			}
		}
		for _, unique := range table.uniques {
			if _, err = tx.CreateBucketIfNotExists([]byte(uniquePrefix + unique)); err != nil {
				table.log.Error("Error creating bucket '%s: %s", uniquePrefix+unique, err.Error())
				return err
			}
		}

		return nil
	})
	if err != nil {
		table.log.Error("Error preparing bolt database: %s", err.Error())
		data.Close()
		return err
	}

	return nil
}

func checkForExistingBoltTable(filePath string) error {
	if _, err := os.Stat(filePath); err != nil {
		return nil
	}

	db, err := bbolt.Open(filePath, os.ModePerm, nil)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.View(func(tx *bbolt.Tx) error {
		defer recover()
		bucket := tx.Bucket(configKey)
		if bucket == nil {
			return fmt.Errorf("%s: %s", ErrBadTableFile, "no bucket found")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func registerGobType(o any) {
	defer panicRecovery()
	gob.Register(o)
}

func panicRecovery() {
	recover()
}
