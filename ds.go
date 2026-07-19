/*
Package ds (short for "data store") is a key-value store with hash indexes. It allows for rudimentary but lightning fast
retrieval of grouped or relevant data without having to iterate over all objects in the store.

Define the primary key, indexed keys, and unique keys as tags on struct fields, and DS takes care of the rest.

Deprecated: github.com/ecnepsnai/ds is deprecated and replaced by git.ecn.io/ian/ds. All users should migrate to
git.ecn.io/ian/ds for continued updates. Tag v1.0.0 is drop-in compatible copy of the last release of
github.com/ecnepsnai/ds.
*/
package ds
