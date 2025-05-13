package ds_test

import (
	"github.com/ecnepsnai/ds"
)

func ExampleTable_StartRead() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	// Get the user with username "example"
	var user *User
	table.StartRead(func(tx ds.IReadTransaction[User]) (err error) {
		user, err = tx.Get("example")
		if err != nil {
			return err // error fetching data
		}
		if user == nil {
			return nil // no user found
		}
		return
	})

	if user == nil {
		panic("No user found!")
	}

	// Use the user object
}

func ExampleTable_StartWrite() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	newUser := User{
		Username: "ian",
		Password: "hunter2",
		Email:    "email@domain",
		Enabled:  true,
	}

	err := table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
		return tx.Add(newUser)
	})
	if err != nil {
		panic(err)
	}
}
