package ds

import (
	"fmt"
	"reflect"
	"strings"
)

// Field describes a field
type Field struct {
	Name string
	Tag  string
	Type string
}

func (table *Table) getFields() []Field {
	return structFieldsToFields(allFields(table.typeOf))
}

func structFieldsToFields(fields []reflect.StructField) []Field {
	newFields := make([]Field, len(fields))
	for i, field := range fields {
		newFields[i] = Field{
			Name: field.Name,
			Tag:  field.Tag.Get("ds"),
			Type: field.Type.Name(),
		}
	}
	return newFields
}

func (table *Table) getKeysFromFields() (string, []string, []string, error) {
	var primaryKey string
	var indexes []string
	var uniques []string

	fields := allFields(table.typeOf)
	if len(fields) == 0 {
		return "", nil, nil, fmt.Errorf("type '%s' has no fields", table.typeOf.Name())
	}
	for _, field := range fields {
		tag := field.Tag.Get("ds")
		if len(tag) == 0 {
			continue
		}
		table.log.Debug("Field: %s, tag: %s", field.Name, tag)

		if strings.Contains(tag, "primary") {
			if len(primaryKey) > 0 {
				table.log.Error("Cannot specify multiple primary keys")
				return "", nil, nil, fmt.Errorf("cannot specify multiple primary keys")
			}

			table.log.Debug("Primary key field '%s'", field.Name)
			primaryKey = field.Name
		} else if strings.Contains(tag, "index") {
			table.log.Debug("Adding indexed field '%s'", field.Name)
			indexes = append(indexes, field.Name)
		} else if strings.Contains(tag, "unique") {
			table.log.Debug("Adding unique field '%s'", field.Name)
			uniques = append(uniques, field.Name)
		} else {
			table.log.Error("Unknown struct tag '%s' on field '%s'", tag, field.Name)
			return "", nil, nil, fmt.Errorf("unknown struct tag '%s' on field '%s'", tag, field.Name)
		}
	}

	if len(primaryKey) <= 0 {
		table.log.Error("A primary key is required")
		return "", nil, nil, fmt.Errorf("a primary key is required")
	}

	return primaryKey, indexes, uniques, nil
}

func compareFields(oldFields, newFields []Field) error {
	var oldPrimary Field
	var newPrimary Field

	for _, newField := range newFields {
		if newField.Tag == "primary" {
			newPrimary = newField
		}

		for _, oldField := range oldFields {
			if oldField.Tag == "primary" {
				oldPrimary = oldField
			}

			if newField.Name != oldField.Name {
				continue
			}

			if newField.Type != oldField.Type {
				return fmt.Errorf("cannot change type of field '%s' without migration. new='%s' old='%s'", newField.Name, oldField.Type, newField.Type)
			}
			if newField.Tag != oldField.Tag {
				return fmt.Errorf("cannot change tag of field '%s' without migration. new='%s' old='%s'", newField.Name, oldField.Tag, newField.Tag)
			}
		}
	}

	if newPrimary.Name != oldPrimary.Name {
		return fmt.Errorf("cannot change name of primary field '%s'", oldPrimary.Name)
	}

	return nil
}

func allFields(typeOf reflect.Type) []reflect.StructField {
	i := 0
	fields := make([]reflect.StructField, typeOf.NumField())
	for i < typeOf.NumField() {
		fields[i] = typeOf.Field(i)
		i++
	}
	return fields
}
