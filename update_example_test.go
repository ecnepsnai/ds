package ds_test

import "github.com/ecnepsnai/ds"

func ExampleIReadWriteTransaction_Update() {
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

	newUser.Password = "something else"

	// Update an existing entry (based on the primary key)
	err = table.StartWrite(func(tx ds.IReadWriteTransaction[User]) error {
		return tx.Update(newUser)
	})
	if err != nil {
		panic(err)
	}
}
