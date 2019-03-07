package ds

import (
	"fmt"

	"github.com/etcd-io/bbolt"
)

// Config describes ds table configuration
type Config struct {
	Name            string
	TypeOf          string
	PrimaryKey      string
	Indexes         []string
	Uniques         []string
	LastInsertIndex uint64
}

func (table *Table) getConfig(tx *bbolt.Tx) (*Config, error) {
	configData := tx.Bucket(configKey).Get(configKey)
	if configData == nil {
		table.log.Error("No config present for table")
		return nil, fmt.Errorf("No config present for table")
	}
	config, err := gobDecodeConfig(configData)
	if err != nil {
		table.log.Error("Error decoding table config: %s", err.Error())
		return nil, err
	}
	return config, nil
}

func (config *Config) update(tx *bbolt.Tx) error {
	bucket := tx.Bucket(configKey)
	data, err := gobEncode(*config)
	if err != nil {
		return err
	}
	return bucket.Put(configKey, data)
}

func (table *Table) initalizeConfig(tx *bbolt.Tx) error {
	configBucket, err := tx.CreateBucketIfNotExists(configKey)
	if err != nil {
		table.log.Error("Error creating config bucket: %s", err.Error())
		return err
	}
	configData := configBucket.Get(configKey)
	if configData == nil {
		// New Table
		config := Config{
			Name:            table.Name,
			TypeOf:          table.typeOf.Name(),
			PrimaryKey:      table.primaryKey,
			Indexes:         table.indexes,
			Uniques:         table.uniques,
			LastInsertIndex: 0,
		}
		data, err := gobEncode(config)
		if err != nil {
			return err
		}
		if err := configBucket.Put(configKey, data); err != nil {
			return err
		}
	} else {
		// Existing Table
		config, err := gobDecodeConfig(configData)
		if err != nil {
			return err
		}
		if config.TypeOf != table.typeOf.Name() {
			table.log.Error("Cannot register type '%s' for existing table for type '%s'", table.typeOf.Name(), config.TypeOf)
			return fmt.Errorf("Cannot register type '%s' for existing table for type '%s'", table.typeOf.Name(), config.TypeOf)
		}
		table.log.Debug("TypeOf matches")
		// I don't know how to test this
		if config.PrimaryKey != table.primaryKey {
			table.log.Error("Cannot change primary key of table")
			return fmt.Errorf("Cannot change primary key of table")
		}
		table.log.Debug("PrimaryKey matches")
	}
	optionsData := configBucket.Get(optionsKey)
	if optionsData == nil {
		data, err := gobEncode(table.options)
		if err != nil {
			return err
		}
		if err := configBucket.Put(optionsKey, data); err != nil {
			return err
		}
	} else {
		options, err := gobDecodeOptions(optionsData)
		if err != nil {
			return err
		}

		if !options.compare(table.options) {
			table.log.Error("Cannot change options of existing table")
			return fmt.Errorf("Cannot change options of existing table")
		}
		table.log.Debug("Config matches")
	}

	return nil
}
