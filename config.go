package ds

import (
	"fmt"

	"go.etcd.io/bbolt"
)

// Config describes ds table configuration
type Config struct {
	Fields          []Field
	TypeOf          string
	PrimaryKey      string
	Indexes         []string
	Uniques         []string
	LastInsertIndex uint64
	Version         int
}

func getTableOptions(tablePath string) (*Options, error) {
	t, err := bbolt.Open(tablePath, 0644, nil)
	if err != nil {
		return nil, err
	}
	defer t.Close()

	var optionsData []byte

	err = t.View(func(tx *bbolt.Tx) error {
		configBucket := tx.Bucket(configKey)
		optionsData = configBucket.Get(optionsKey)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(optionsData) == 0 {
		return nil, nil
	}

	return gobDecodeOptions(optionsData)
}

func (table *Table) getConfig(tx *bbolt.Tx) (*Config, error) {
	configData := tx.Bucket(configKey).Get(configKey)
	if configData == nil {
		table.log.Error("No config present for table")
		return nil, fmt.Errorf("no config present for table")
	}
	config, err := gobDecodeConfig(configData)
	if err != nil {
		table.log.PError("Error decoding table config", map[string]interface{}{
			"error": err.Error(),
		})
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

func (table *Table) initializeConfig(tx *bbolt.Tx, force bool) error {
	configBucket, err := tx.CreateBucketIfNotExists(configKey)
	if err != nil {
		table.log.PError("Error creating config bucket", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}
	configData := configBucket.Get(configKey)
	if configData == nil {
		// New Table
		config := Config{
			Fields:          table.getFields(),
			TypeOf:          table.typeOf.Name(),
			PrimaryKey:      table.primaryKey,
			Indexes:         table.indexes,
			Uniques:         table.uniques,
			LastInsertIndex: 0,
			Version:         currentDSSchemaVersion,
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
		if err := compareFields(table.getFields(), config.Fields); err != nil {
			table.log.Error("%s", err.Error())
			return err
		}
		if config.Version > currentDSSchemaVersion {
			table.log.Error("Unable to register existing table %s as it's from a newer version of DS. %d > %d", table.Name, config.Version, currentDSSchemaVersion)
			return fmt.Errorf("table is from newer version of ds")
		}
		table.log.Debug("TypeOf matches")
		if !force && config.PrimaryKey != table.primaryKey {
			table.log.Error("Cannot change primary key of table")
			return fmt.Errorf("cannot change primary key of table")
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
			return fmt.Errorf("cannot change options of existing table")
		}
		table.log.Debug("Config matches")
	}

	return nil
}
