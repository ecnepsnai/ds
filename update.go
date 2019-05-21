package ds

import "github.com/etcd-io/bbolt"

// Update will update an existing object in the table. The primary key must match for this object
// otherwise it will just be inserted as a new object.
func (table *Table) Update(o interface{}) error {

	err := table.data.Update(func(tx *bbolt.Tx) error {
		var index *uint64
		if !table.options.DisableSorting {
			i, err := table.indexForObject(tx, o)
			if err != nil {
				return err
			}
			index = i
		}

		if err := table.delete(tx, o); err != nil {
			return err
		}
		if err := table.add(tx, o); err != nil {
			return err
		}

		if !table.options.DisableSorting {
			if index != nil {
				if err := table.setInsertIndexForObject(tx, o, *index); err != nil {
					return err
				}
			}

			config, err := table.getConfig(tx)
			if err != nil {
				return err
			}
			config.LastInsertIndex = config.LastInsertIndex - 1
			if err := config.update(tx); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		table.log.Error("Error updating entry: %s", err.Error())
		return err
	}

	return nil
}
