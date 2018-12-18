package ds

// Update will update an existing object in the table. The primary key must match for this object
// otherwise it will just be inserted as a new object.
func (table *Table) Update(o interface{}) error {
	if err := table.Delete(o); err != nil {
		return err
	}
	if err := table.Add(o); err != nil {
		return err
	}

	return nil
}
