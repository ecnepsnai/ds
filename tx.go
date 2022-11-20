package ds

// IReadTransaction describes an interface for performing read-only operations on a table.
type IReadTransaction interface {
	// Get will get a single entry by its primary key. Returns (nil, nil) if nothing found.
	Get(primaryKey interface{}) (interface{}, error)
	// GetIndex will get multiple entries that contain the same value for the specified indexed field.
	// Result is not ordered. Use GetIndexSorted to return a sorted slice.
	// Returns an empty array if nothing found.
	GetIndex(fieldName string, value interface{}, options *GetOptions) ([]interface{}, error)
	// GetUnique will get a single entry based on the value of the provided unique field.
	// Returns (nil, nil) if nothing found.
	GetUnique(fieldName string, value interface{}) (interface{}, error)
	// GetAll will get all of the entries in the table.
	GetAll(options *GetOptions) ([]interface{}, error)
}

// IReadWriteTransaction describes an interface for performing read or write operations on a table.
type IReadWriteTransaction interface {
	IReadTransaction
	// Add will add a new object to the table. o must the the same type that was used to register the table and cannot be a pointer.
	Add(o interface{}) error
	// Delete will delete the provided object and clean indexes
	Delete(o interface{}) error
	// DeletePrimaryKey will delete the object with the associated primary key and clean indexes. Does nothing if not object
	// matches the given primary key.
	DeletePrimaryKey(o interface{}) error
	// DeleteUnique will delete the object with the associated unique value and clean indexes. Does nothing if no object
	// matched the given unique fields value.
	DeleteUnique(field string, o interface{}) error
	// DeleteAllIndex will delete all objects matching the given indexed fields value
	DeleteAllIndex(fieldName string, value interface{}) error
	// DeleteAll delete all objects from the table
	DeleteAll() error
	// Update will update an existing object in the table. The primary key must match for this object
	// otherwise it will just be inserted as a new object. Updated objects do not change positions in a sorted
	// table.
	Update(o interface{}) error
}

type readTxImpl struct{ table *Table }
type readWriteTxImpl struct{ table *Table }

// StartRead will start a new read-only transaction on the table. ctx will be called when the transaction is ready.
// This method may block if there is an active write transaction.
func (table *Table) StartRead(ctx func(tx IReadTransaction) error) error {
	table.txLock.RLock()
	defer func() {
		table.txLock.RUnlock()
		recover()
	}()
	return ctx(readTxImpl{table})
}

// StartWrite will start a new read-write transaction on the table. ctx will be called when the transaction is ready.
// This method may block if there is an active transaction.
//
// Note that any any errors or partial changes made during ctx are not reverted.
func (table *Table) StartWrite(ctx func(tx IReadWriteTransaction) error) error {
	table.txLock.Lock()
	defer func() {
		table.txLock.Unlock()
		recover()
	}()
	return ctx(readWriteTxImpl{table})
}

func (tx readTxImpl) Get(primaryKey interface{}) (interface{}, error) {
	return tx.table.get(primaryKey)
}

func (tx readTxImpl) GetIndex(fieldName string, value interface{}, options *GetOptions) ([]interface{}, error) {
	return tx.table.getIndex(fieldName, value, options)
}

func (tx readTxImpl) GetUnique(fieldName string, value interface{}) (interface{}, error) {
	return tx.table.getUnique(fieldName, value)
}

func (tx readTxImpl) GetAll(options *GetOptions) ([]interface{}, error) {
	return tx.table.getAll(options)
}

func (tx readWriteTxImpl) Get(primaryKey interface{}) (interface{}, error) {
	return tx.table.get(primaryKey)
}

func (tx readWriteTxImpl) GetIndex(fieldName string, value interface{}, options *GetOptions) ([]interface{}, error) {
	return tx.table.getIndex(fieldName, value, options)
}

func (tx readWriteTxImpl) GetUnique(fieldName string, value interface{}) (interface{}, error) {
	return tx.table.getUnique(fieldName, value)
}

func (tx readWriteTxImpl) GetAll(options *GetOptions) ([]interface{}, error) {
	return tx.table.getAll(options)
}

func (tx readWriteTxImpl) Add(o interface{}) error {
	return tx.table.add(o)
}

func (tx readWriteTxImpl) Delete(o interface{}) error {
	return tx.table.delete(o)
}

func (tx readWriteTxImpl) DeletePrimaryKey(o interface{}) error {
	return tx.table.deletePrimaryKey(o)
}

func (tx readWriteTxImpl) DeleteUnique(field string, o interface{}) error {
	return tx.table.deleteUnique(field, o)
}

func (tx readWriteTxImpl) DeleteAllIndex(fieldName string, value interface{}) error {
	return tx.table.deleteAllIndex(fieldName, value)
}

func (tx readWriteTxImpl) DeleteAll() error {
	return tx.table.deleteAll()
}

func (tx readWriteTxImpl) Update(o interface{}) error {
	return tx.table.update(o)
}
