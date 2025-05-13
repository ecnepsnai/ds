package ds_test

import "github.com/ecnepsnai/ds"

func ExampleIReadWriteTransaction_Delete() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	deleteUser := User{
		Username: "ian",
		Password: "hunter2",
		Email:    "email@domain",
		Enabled:  true,
	}

	// Delete the object
	err := table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
		return tx.Delete(deleteUser)
	})

	if err != nil {
		panic(err)
	}
}

func ExampleIReadWriteTransaction_DeletePrimaryKey() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	// Delete an object by its primary key
	err := table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
		return tx.DeletePrimaryKey("ian")
	})
	if err != nil {
		panic(err)
	}
}

func ExampleIReadWriteTransaction_DeleteUnique() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	// Delete an object by a unique fields value
	err := table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
		return tx.DeleteUnique("Email", "user@domain")
	})
	if err != nil {
		panic(err)
	}
}

func ExampleIReadWriteTransaction_DeleteAllIndex() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	// Delete all objects with the following indexed fields value
	err := table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
		return tx.DeleteAllIndex("Enabled", false)
	})
	if err != nil {
		panic(err)
	}
}

func ExampleIReadWriteTransaction_DeleteAll() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	// Delete all objects
	err := table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
		return tx.DeleteAll()
	})
	if err != nil {
		panic(err)
	}
}
