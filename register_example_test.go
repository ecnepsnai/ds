package ds_test

import "github.com/ecnepsnai/ds"

func ExampleRegister() {
	type User struct {
		// Primary fields represent the primary key of the object. Your object must have exactly one primary field
		// and its value is unique
		Username string `ds:"primary"`
		// Unique fields function just like primary fields except any field (other than the primary field) can be unique
		Email string `ds:"unique"`
		// Index fields represent fields where objects with identical values are grouped together so they can be fetched
		// quickly later
		Enabled bool `ds:"index"`
		// Fields with no ds tag are saved, but you can't fetch based on their value, and can have duplicate values
		// between entries
		Password string
	}

	tablePath := "user.db"

	table, err := ds.Register[User](User{}, tablePath, nil)
	if err != nil {
		panic(err)
	}

	// Don't forget to close your table when you're finished
	table.Close()
}
