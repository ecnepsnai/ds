package ds_test

import (
	"fmt"

	"github.com/ecnepsnai/ds"
)

func ExampleIReadTransaction_Get() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
	}

	var user *User
	table.StartRead(func(tx ds.IReadTransaction) error {
		object, err := tx.Get("ian")
		if err != nil {
			return err
		}
		if object == nil {
			// No object with that primary key found
			return nil
		}

		u, ok := object.(User)
		if !ok {
			// The object wasn't a `User`
			panic("invalid type")
		}
		user = &u
		return nil
	})

	fmt.Printf("Username: %s\n", user)
}

func ExampleIReadTransaction_GetUnique() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
		Email    string `ds:"unique"`
	}

	var user *User
	table.StartRead(func(tx ds.IReadTransaction) error {
		// Get the user with the email user@domain
		object, err := tx.GetUnique("Email", "user@domain")
		if err != nil {
			return err
		}
		if object == nil {
			// No object with that primary key found
			return nil
		}

		u, ok := object.(User)
		if !ok {
			// The object wasn't a `User`
			panic("invalid type")
		}
		user = &u
		return nil
	})

	fmt.Printf("Username: %s\n", user)
}

func ExampleIReadTransaction_GetAll() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
	}

	var users []User
	table.StartRead(func(tx ds.IReadTransaction) error {
		// Get only the first 100 users sorted by when they were added
		objects, err := tx.GetAll(&ds.GetOptions{
			Sorted:    true,
			Ascending: true,
			Max:       100,
		})
		if err != nil {
			return err
		}
		if objects == nil {
			// No objects were returned
			return nil
		}

		users = make([]User, len(objects))
		for i, object := range objects {
			user, ok := object.(User)
			if !ok {
				// The object wasn't a `User`
				panic("invalid type")
			}
			users[i] = user
		}
		return nil
	})
}

func ExampleIReadTransaction_GetIndex() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
		Enabled  bool   `ds:"index"`
	}

	var users []User
	table.StartRead(func(tx ds.IReadTransaction) error {
		// Get all enabled users
		objects, err := tx.GetIndex("Enabled", true, nil)
		if err != nil {
			return err
		}
		if objects == nil {
			// No objects were returned
			return nil
		}

		users = make([]User, len(objects))
		for i, object := range objects {
			user, ok := object.(User)
			if !ok {
				// The object wasn't a `User`
				panic("invalid type")
			}
			users[i] = user
		}
		return nil
	})
}
