package ds

// Error constant
const (
	ErrMissingRequiredValue     = "missing required value"
	ErrPointer                  = "pointer provided when none expected"
	ErrPropertyChanged          = "property changed"
	ErrDuplicatePrimaryKey      = "duplicate value for primary key"
	ErrDuplicateUnique          = "duplicate value for unique field"
	ErrFieldNotIndexed          = "field not indexed"
	ErrFieldNotUnique           = "field not unique"
	ErrMigrateTablePathNotFound = "table path"
	ErrBadTableFile             = "bad table file"
)
