package ds

import (
	"encoding/binary"
	"reflect"

	"github.com/etcd-io/bbolt"
)

func (table *Table) indexForPrimaryKey(tx *bbolt.Tx, primaryKey []byte) *uint64 {
	indexBucket := tx.Bucket(insertOrderKey)
	b := indexBucket.Get(primaryKey)
	if len(b) == 0 {
		return nil
	}
	index := binary.LittleEndian.Uint64(b)
	return &index
}

func (table *Table) indexForObject(tx *bbolt.Tx, object interface{}) (*uint64, error) {
	pk := reflect.ValueOf(object).FieldByName(table.primaryKey).Interface()
	pkBytes, err := gobEncode(pk)
	if err != nil {
		return nil, err
	}
	return table.indexForPrimaryKey(tx, pkBytes), nil
}
