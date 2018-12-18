package ds

import (
	"path"
	"testing"
)

// Test that an object can be updated
func TestUpdate(t *testing.T) {
	type exampleType struct {
		Primary string `ds:"primary"`
		Index   string `ds:"index"`
		Unique  string `ds:"unique"`
	}

	table, err := Register(exampleType{}, path.Join(tmpDir, randomString(12)))
	if err != nil {
		t.Errorf("Error registering table: %s", err.Error())
	}

	object := exampleType{
		Primary: randomString(12),
		Index:   randomString(12),
		Unique:  randomString(12),
	}

	err = table.Add(object)
	if err != nil {
		t.Errorf("Error adding value to table: %s", err.Error())
	}

	object.Index = randomString(12)

	err = table.Update(object)
	if err != nil {
		t.Errorf("Error updating value to table: %s", err.Error())
	}
}
