package ds_test

import "github.com/ecnepsnai/ds"

func ExampleTable_IsIndexed() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
		Email    string `ds:"email"`
		Enabled  bool   `ds:"index"`
	}

	table.IsIndexed("Username") // returns False
	table.IsIndexed("Enabled")  // returns True
}

func ExampleTable_IsUnique() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
		Email    string `ds:"email"`
		Enabled  bool   `ds:"index"`
	}

	table.IsUnique("Username") // returns False
	table.IsUnique("Email")    // returns True
}
