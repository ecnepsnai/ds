package ds_test

import "github.com/ecnepsnai/ds"

func ExampleTable_IsIndexed() {
	type User struct {
		Username string `ds:"primary"`
		Email    string `ds:"email"`
		Enabled  bool   `ds:"index"`
	}

	tablePath := "user.db"
	table, err := ds.Register(User{}, tablePath, nil)
	if err != nil {
		panic(err)
	}

	table.IsIndexed("Username") // returns False
	table.IsIndexed("Enabled")  // returns True
}

func ExampleTable_IsUnique() {
	type User struct {
		Username string `ds:"primary"`
		Email    string `ds:"email"`
		Enabled  bool   `ds:"index"`
	}

	tablePath := "user.db"
	table, err := ds.Register(User{}, tablePath, nil)
	if err != nil {
		panic(err)
	}

	table.IsUnique("Username") // returns False
	table.IsUnique("Email")    // returns True
}
