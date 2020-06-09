package ds_test

import "github.com/ecnepsnai/ds"

func ExampleTable_Delete() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	deleteUser := User{
		Username: "ian",
		Password: "hunter2",
		Email:    "email@domain",
		Enabled:  true,
	}

	// Delete the object
	if err := table.Delete(deleteUser); err != nil {
		panic(err)
	}
}

func ExampleTable_DeletePrimaryKey() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	// Delete an object by its primary key
	if err := table.DeletePrimaryKey("ian"); err != nil {
		panic(err)
	}
}

func ExampleTable_DeleteUnique() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	// Delete an object by a unique fields value
	if err := table.DeleteUnique("Email", "user@domain"); err != nil {
		panic(err)
	}
}

func ExampleTable_DeleteAllIndex() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	// Delete all objects with the following indexed fields value
	if err := table.DeleteAllIndex("Enabled", false); err != nil {
		panic(err)
	}
}

func ExampleTable_DeleteAll() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	// Delete all objects
	if err := table.DeleteAll(); err != nil {
		panic(err)
	}
}
