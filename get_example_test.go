package ds_test

import (
	"fmt"

	"github.com/ecnepsnai/ds"
)

func ExampleIReadTransaction_Get() {
	type User struct {
		Username string `ds:"primary"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	var user *User
	table.StartRead(func(tx ds.IReadTransaction[User]) (err error) {
		user, err = tx.Get("ian")
		if err != nil {
			return err
		}
		return
	})
	if user == nil {
		// No object with that primary key found
		panic("No user found")
	}

	fmt.Printf("Username: %s\n", user)
}

func ExampleIReadTransaction_GetUnique() {
	type User struct {
		Username string `ds:"primary"`
		Email    string `ds:"unique"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	var user *User
	table.StartRead(func(tx ds.IReadTransaction[User]) (err error) {
		// Get the user with the email user@domain
		user, err = tx.GetUnique("Email", "user@domain")
		if err != nil {
			return err
		}
		return
	})
	if user == nil {
		// No object with that primary key found
		panic("No user found")
	}

	fmt.Printf("Username: %s\n", user)
}

func ExampleIReadTransaction_GetAll() {
	type User struct {
		Username string `ds:"primary"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an example

	var users []*User
	table.StartRead(func(tx ds.IReadTransaction[User]) (err error) {
		// Get only the first 100 users sorted by when they were added
		users, err = tx.GetAll(&ds.GetOptions{
			Sorted:    true,
			Ascending: true,
			Max:       100,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if users == nil {
		panic("No entries returned")
	}

	fmt.Printf("Users: %d", len(users))
}

func ExampleIReadTransaction_GetIndex() {
	type User struct {
		Username string `ds:"primary"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table[User] // Assumes the table is already registered, see ds.Register for an exampl

	var users []*User
	table.StartRead(func(tx ds.IReadTransaction[User]) (err error) {
		// Get all enabled users
		users, err = tx.GetIndex("Enabled", true, nil)
		if err != nil {
			return err
		}
		return
	})
	if users == nil {
		panic("No entries returned")
	}

	fmt.Printf("Users: %d", len(users))
}
