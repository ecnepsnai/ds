package ds

const (
	indexPrefix  = "index:"
	uniquePrefix = "unique:"
)

var dataKey = []byte("data")
var insertOrderKey = []byte("insert_order")
var configKey = []byte("config")
var optionsKey = []byte("options")
