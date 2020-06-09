package ds_test

import "github.com/ecnepsnai/ds"

func ExampleTable_Add() {
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
}
