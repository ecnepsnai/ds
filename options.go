package ds

// Options describes options for DS tables. Once set, these cannot be changed.
type Options struct {
	// DisableSorting disable all sorting features. This will make tables smaller, and inserts/removes/deletes faster.
	DisableSorting bool

	force bool
}

func (options Options) compare(o Options) bool {
	if options.DisableSorting != o.DisableSorting {
		return false
	}

	return true
}
