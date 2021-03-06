package ds_test

import "github.com/ecnepsnai/ds"

func ExampleTable_Update() {
	type User struct {
		Username string `ds:"primary"`
		Password string
		Email    string `ds:"unique"`
		Enabled  bool   `ds:"index"`
	}

	var table *ds.Table // Assumes the table is already registered, see ds.Register for an example

	newUser := User{
		Username: "ian",
		Password: "hunter2",
		Email:    "email@domain",
		Enabled:  true,
	}
	if err := table.Add(newUser); err != nil {
		panic(err)
	}

	newUser.Password = "something else"

	// Update an existing entry (based on the primary key)
	if err := table.Update(newUser); err != nil {
		panic(err)
	}
}
