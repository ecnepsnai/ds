package ds

import (
	"encoding/binary"
	"reflect"

	"github.com/etcd-io/bbolt"
)

func (table *Table) indexForPrimaryKey(tx *bbolt.Tx, primaryKey []byte) uint64 {
	indexBucket := tx.Bucket(insertOrderKey)
	b := indexBucket.Get(primaryKey)
	index := binary.LittleEndian.Uint64(b)
	return index
}

func (table *Table) indexForObject(tx *bbolt.Tx, object interface{}) (uint64, error) {
	pk := reflect.ValueOf(object).FieldByName(table.primaryKey).Interface()
	pkBytes, err := gobEncode(pk)
	if err != nil {
		return 0, err
	}
	return table.indexForPrimaryKey(tx, pkBytes), nil
}
