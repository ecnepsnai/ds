/*
Package ds (short for "data store") is a pure-go key-value store with hash indexes. It allows for rudimentary but
lightning fast retrieval of grouped or relevant data without having to iterate over all objects in the store.

Define the primary key, indexed keys, and unique keys as tags on struct fields, and DS takes care of the rest.
*/
package ds
