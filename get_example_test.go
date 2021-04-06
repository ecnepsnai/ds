package ds_test

import (
	"fmt"

	"github.com/ecnepsnai/ds"
)

func ExampleTable_Get() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
	}

	object, err := table.Get("ian")
	if err != nil {
		panic(err)
	}
	if object == nil {
		// No object with that primary key found
	}

	user, ok := object.(User)
	if !ok {
		// The object wasn't a `User`
	}

	fmt.Printf("Username: %s\n", user)
}

func ExampleTable_GetUnique() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
		Email    string `ds:"unique"`
	}

	// Get the user with the email user@domain
	object, err := table.GetUnique("Email", "user@domain")
	if err != nil {
		panic(err)
	}
	if object == nil {
		// No object with that primary key found
	}

	user, ok := object.(User)
	if !ok {
		// The object wasn't a `User`
	}

	fmt.Printf("Username: %s\n", user)
}

func ExampleTable_GetAll() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
	}

	// Get only the first 100 users sorted by when they were added
	objects, err := table.GetAll(&ds.GetOptions{
		Sorted:    true,
		Ascending: true,
		Max:       100,
	})
	if err != nil {
		panic(err)
	}
	if objects == nil {
		// No objects were returned
		return
	}

	users := make([]User, len(objects))
	for i, object := range objects {
		user, ok := object.(User)
		if !ok {
			// The object wasn't a `User`
		}
		users[i] = user
	}
}

func ExampleTable_GetIndex() {
	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	type User struct {
		Username string `ds:"primary"`
		Enabled  bool   `ds:"index"`
	}

	// Get all enabled users
	objects, err := table.GetIndex("Enabled", true, nil)
	if err != nil {
		panic(err)
	}
	if objects == nil {
		// No objects were returned
		return
	}

	users := make([]User, len(objects))
	for i, object := range objects {
		user, ok := object.(User)
		if !ok {
			// The object wasn't a `User`
		}
		users[i] = user
	}
}
