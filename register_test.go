package ds

import (
	"path"
	"testing"
)

func TestRegister(t *testing.T) {
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
}

func TestRegisterMultiplePrimaryKey(t *testing.T) {
	type exampleType struct {
		Primary  string `ds:"primary"`
		Primary2 string `ds:"primary"`
	}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err == nil {
		t.Errorf("No error seen while attempting to register type with multiple primary keys")
	}
}

func TestRegisterNoPrimaryKey(t *testing.T) {
	type exampleType struct {
		Index  string `ds:"index"`
		Unique string `ds:"unique"`
	}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err == nil {
		t.Errorf("No error seen while attempting to register type with no primary keys")
	}
}

func TestRegisterMultipleOfSameType(t *testing.T) {
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
}

func TestRegisterNoExportedFields(t *testing.T) {
	type exampleType struct {
		primary string `ds:"primary"`
		index   string `ds:"index"`
		unique  string `ds:"unique"`
	}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}
}

func TestRegisterNoFields(t *testing.T) {
	type exampleType struct{}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err == nil {
		t.Errorf("No error seen while attempting to register type with no fields")
	}
}

func TestRegisterUnknownStructTag(t *testing.T) {
	type exampleType struct {
		Unknown string `ds:"💩"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	if _, err := Register(exampleType{}, path.Join(tmpDir, randomString(12))); err == nil {
		t.Errorf("No error seen while attempting to register type with unknown struct tag")
	}
}

func TestRegisterPointer(t *testing.T) {
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	object := exampleType{}

	if _, err := Register(&object, path.Join(tmpDir, randomString(12))); err == nil {
		t.Errorf("No error seen while attempting to register pointer")
	}
}
