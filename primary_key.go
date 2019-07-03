package ds

import "reflect"

func (table *Table) primaryKeyBytes(o interface{}) ([]byte, error) {
	valueOf := reflect.Indirect(reflect.ValueOf(o))
	primaryKeyValue := valueOf.FieldByName(table.primaryKey)
	primaryKeyBytes, err := gobEncode(primaryKeyValue.Interface())
	if err != nil {
		table.log.Error("Cannot encode primary key value: %s", err.Error())
		return nil, err
	}
	return primaryKeyBytes, nil
}
