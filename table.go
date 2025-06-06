package ds

import (
	"reflect"
	"sync"

	"github.com/ecnepsnai/logtic"
	"go.etcd.io/bbolt"
)

// Table describes a ds table. A table is mapped to a single registered object type and contains
// both the data and the indexes.
type Table[T any] struct {
	Name       string
	typeOf     reflect.Type
	log        *logtic.Source
	primaryKey string
	indexes    []string
	uniques    []string
	data       *bbolt.DB
	options    Options
	txLock     *sync.RWMutex
}

// Close will close the table. This will not panic if the table has not been opened or already been closed.
func (table *Table[T]) Close() {
	if table != nil && table.data != nil {
		go tryCloseData(table.data)
	}
}

func tryCloseData(data *bbolt.DB) {
	defer panicRecovery()
	data.Close()
}

// IsIndexed is the given field indexed
func (table *Table[T]) IsIndexed(field string) bool {
	for _, index := range table.indexes {
		if index == field {
			return true
		}
	}
	return false
}

// IsUnique is the given field unique
func (table *Table[T]) IsUnique(field string) bool {
	for _, unique := range table.uniques {
		if unique == field {
			return true
		}
	}
	return false
}
