> [!CAUTION]
> `github.com/ecnepsnai/ds` is deprecated and replaced by [git.ecn.io/ian/ds](https://git.ecn.io/ian/ds).
> All users should migrate to `git.ecn.io/ian/ds` for continued updates. Tag v1.0.0 is drop-in compatible copy of the
> last release of `github.com/ecnepsnai/ds`.

# DS

[![Go Report Card](https://goreportcard.com/badge/github.com/ecnepsnai/ds?style=flat-square)](https://goreportcard.com/report/github.com/ecnepsnai/ds)
[![Godoc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://pkg.go.dev/github.com/ecnepsnai/ds)
[![Releases](https://img.shields.io/github/release/ecnepsnai/ds/all.svg?style=flat-square)](https://github.com/ecnepsnai/ds/releases)
[![LICENSE](https://img.shields.io/github/license/ecnepsnai/ds.svg?style=flat-square)](https://github.com/ecnepsnai/ds/blob/master/LICENSE)

Package ds (short for "data store") is a key-value store with hash indexes. It allows for rudimentary but lightning fast
retrieval of grouped or relevant data without having to iterate over all objects in the store.

Define the primary key, indexed keys, and unique keys as tags on struct fields, and DS takes care of the rest.

# Usage & Examples

Examples can be found on the [documentation for the library](https://pkg.go.dev/github.com/ecnepsnai/ds)
