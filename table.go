package ds

import (
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/ecnepsnai/logtic"
)

// Table describes a ds table. A table is mapped to a single registered object type and contains
// both the data and the indexes.
type Table struct {
	Name       string
	typeOf     reflect.Type
	log        *logtic.Source
	primaryKey string
	indexes    []string
	uniques    []string
	data       *bolt.DB
}

// IsIndexed is the given field indexes
func (table *Table) IsIndexed(field string) bool {
	for _, index := range table.indexes {
		if index == field {
			return true
		}
	}
	return false
}

// IsUnique is the given field unique
func (table *Table) IsUnique(field string) bool {
	for _, unique := range table.uniques {
		if unique == field {
			return true
		}
	}
	return false
}
