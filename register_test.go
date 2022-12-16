package ds_test

import (
	"bytes"
	"encoding/gob"
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/ds"
	"go.etcd.io/bbolt"
)

func TestRegister(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
}

func TestRegisterMultiplePrimaryKey(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary  string `ds:"primary"`
		Primary2 string `ds:"primary"`
	}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err == nil {
		t.Errorf("No error seen while attempting to register type with multiple primary keys")
	}
}

func TestRegisterNoPrimaryKey(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Index  string `ds:"index"`
		Unique string `ds:"unique"`
	}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err == nil {
		t.Errorf("No error seen while attempting to register type with no primary keys")
	}
}

func TestRegisterMultipleOfSameType(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
}

func TestRegisterNoExportedFields(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		primary string `ds:"primary"`
		index   string `ds:"index"`
		unique  string `ds:"unique"`
	}

	if _, err := ds.Register(exampleType{
		primary: randomString(6),
		index:   randomString(6),
		unique:  randomString(6),
	}, path.Join(t.TempDir(), randomString(12)), nil); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
}

func TestRegisterNoFields(t *testing.T) {
	t.Parallel()

	type exampleType struct{}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err == nil {
		t.Errorf("No error seen while attempting to register type with no fields")
	}
}

func TestRegisterOtherTags(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary       string `ds:"primary"`
		SomethingElse string `json:"something_else"`
	}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
}

func TestRegisterUnknownStructTag(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Unknown string `ds:"💩"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	if _, err := ds.Register(exampleType{}, path.Join(t.TempDir(), randomString(12)), nil); err == nil {
		t.Errorf("No error seen while attempting to register type with unknown struct tag")
	}
}

func TestRegisterPointer(t *testing.T) {
	t.Parallel()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	object := exampleType{}

	if _, err := ds.Register(&object, path.Join(t.TempDir(), randomString(12)), nil); err == nil {
		t.Errorf("No error seen while attempting to register pointer")
	}
}

func TestRegisterOpenClose(t *testing.T) {
	t.Parallel()

	primaryKey := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}
	dsPath := path.Join(t.TempDir(), randomString(12))

	table, err := ds.Register(exampleType{}, dsPath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction) error {
		return tx.Add(exampleType{
			Primary: primaryKey,
			Index:   randomString(12),
			Unique:  randomString(12),
		})
	})
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	table.Close()
	table = nil

	table, err = ds.Register(exampleType{}, dsPath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	var v interface{}
	err = table.StartRead(func(tx ds.IReadTransaction) error {
		v, err = tx.Get(primaryKey)
		return err
	})
	if err != nil {
		t.Errorf("Error getting object: %s", err.Error())
	}
	got := v.(exampleType).Primary
	if got != primaryKey {
		t.Errorf("Incorrect primary key returned. Expected '%s' got '%s", primaryKey, got)
	}
}

func TestRegisterLockedFile(t *testing.T) {
	t.Parallel()

	dsPath := path.Join(t.TempDir(), randomString(12))
	file, err := os.OpenFile(dsPath, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Errorf("Error making test file: %s", err.Error())
	}
	file.Close()

	if err := os.Chmod(dsPath, 0000); err != nil {
		t.Errorf("Error locking test file: %s", err.Error())
	}

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}
	if _, err := ds.Register(exampleType{}, dsPath, nil); err == nil {
		t.Errorf("No error seen while attempting to open file without permission")
	}
}

func TestRegisterWrongType(t *testing.T) {
	t.Parallel()

	primaryKey := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}
	dsPath := path.Join(t.TempDir(), randomString(12))

	table, err := ds.Register(exampleType{}, dsPath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	err = table.StartWrite(func(tx ds.IReadWriteTransaction) error {
		return tx.Add(exampleType{
			Primary: primaryKey,
			Index:   randomString(12),
			Unique:  randomString(12),
		})
	})

	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	table.Close()
	table = nil

	type otherType struct {
		Something string `ds:"primary"`
		Wicked    string `ds:"index"`
	}

	_, err = ds.Register(otherType{}, dsPath, nil)
	if err == nil {
		t.Errorf("No error seen while registering table with wrong type")
	}
}

func TestRegisterChangeOptions(t *testing.T) {
	t.Parallel()

	primaryKey := randomString(12)
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}
	dsPath := path.Join(t.TempDir(), randomString(12))

	table, err := ds.Register(exampleType{}, dsPath, &ds.Options{
		DisableSorting: true,
	})
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	table.StartWrite(func(tx ds.IReadWriteTransaction) error {
		err = tx.Add(exampleType{
			Primary: primaryKey,
			Index:   randomString(12),
			Unique:  randomString(12),
		})
		if err != nil {
			t.Errorf("Error adding value to table: %s", err.Error())
		}
		return nil
	})

	table.Close()
	table = nil

	_, err = ds.Register(exampleType{}, dsPath, &ds.Options{
		DisableSorting: false,
	})
	if err == nil {
		t.Errorf("No error seen while registering table with changed options")
	}
}

// Test that registering a DS on an existing, unknown bolt table will return an error
func TestRegisterExistingBoltTable(t *testing.T) {
	t.Parallel()

	dsPath := path.Join(t.TempDir(), randomString(12))
	db, err := bbolt.Open(dsPath, 0644, nil)
	if err != nil {
		t.Errorf("Error opening bolt db: %s", err.Error())
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(randomString(12)))
		if err != nil {
			return err
		}
		return bucket.Put([]byte(randomString(12)), []byte(randomString(12)))
	})
	if err != nil {
		t.Errorf("Error adding data to table: %s", err.Error())
	}
	db.Close()

	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	_, err = ds.Register(exampleType{}, dsPath, &ds.Options{
		DisableSorting: true,
	})
	if err == nil {
		t.Errorf("No error seen when expected")
	}
}

func TestRegisterChangePrimaryKey(t *testing.T) {
	t.Parallel()

	tablePath := path.Join(t.TempDir(), randomString(12))

	// Register first table
	table, err := ds.Register(struct {
		Primary string `ds:"primary"`
	}{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
	table.Close()

	// Try to change the primary key
	_, err = ds.Register(struct {
		Secondary string `ds:"primary"`
	}{}, tablePath, nil)
	if err == nil {
		t.Errorf("No error seen when one expected while changing the tables primary key")
	}
}

func TestRegisterChangeField(t *testing.T) {
	t.Parallel()

	tablePath := path.Join(t.TempDir(), randomString(12))

	// Register first table
	table, err := ds.Register(struct {
		Primary string `ds:"primary"`
	}{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
	table.Close()

	// Change the type of Primary to an int
	_, err = ds.Register(struct {
		Primary int `ds:"primary"`
	}{}, tablePath, nil)
	if err == nil {
		t.Errorf("No error seen when one expected while changing a field type")
	}
}

func TestRegisterChangeTag(t *testing.T) {
	t.Parallel()

	tablePath := path.Join(t.TempDir(), randomString(12))

	// Register first table
	table, err := ds.Register(struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
	}{}, tablePath, nil)
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
	table.Close()

	// Change the type of Primary to an int
	_, err = ds.Register(struct {
		Primary string `ds:"primary"`
		Index   string `ds:"unique"`
	}{}, tablePath, nil)
	if err == nil {
		t.Errorf("No error seen when one expected while changing a field tag")
	}
}

func TestRegisterOldVersion(t *testing.T) {
	tablePath := path.Join(t.TempDir(), randomString(12))

	data, err := bbolt.Open(tablePath, os.ModePerm, nil)
	if err != nil {
		t.Fatalf("Error opening bolt database: %s", err.Error())
	}

	err = data.Update(func(tx *bbolt.Tx) error {
		configBucket, err := tx.CreateBucketIfNotExists([]byte("config"))
		if err != nil {
			t.Fatalf("Error creating config bucket: %s", err.Error())
		}

		config := ds.Config{
			Fields: []ds.Field{
				{
					Name: "Primary",
					Tag:  "primary",
					Type: "string",
				},
				{
					Name: "Index",
					Tag:  "index",
					Type: "string",
				},
			},
			TypeOf:          "",
			PrimaryKey:      "Primary",
			Indexes:         []string{"Index"},
			Uniques:         []string{},
			LastInsertIndex: 0x0,
			Version:         65535,
		}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(config); err != nil {
			t.Fatalf("Error encoding config object: %s", err.Error())
		}
		if err := configBucket.Put([]byte("config"), buf.Bytes()); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Error updating table: %s", err.Error())
	}
	data.Close()

	table, err := ds.Register(struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
	}{}, tablePath, nil)
	if err == nil {
		t.Errorf("No error seen when trying to open table from newer version of DS")
	}
	table.Close()
}
