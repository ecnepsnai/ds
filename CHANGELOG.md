# v1.8.3

- Fixed bug where nested struct fields could not change properties during a migration

# v1.8.2

- Update dependencies

# v1.8.1

- Removed deprecated methods

# v1.8.0

**This Release Contains Breaking Changes**

- [**Breaking**] All table read/write operations are deprecated `Get()`, `GetIndex()`, `GetUnique()`, `GetAll()`, `Add()`, `Delete()`, `DeletePrimaryKey()`, `DeleteUnique()`, `DeleteAllIndex()`, `DeleteAll()`, `Update()`. Instead, you must use a read or write transaction: `table.StartRead()` or `table.StartWrite()`. The deprecated methods will be removed in the next major release.

    This change, while large, enables applications to have proper read/write safety across threads. Prior to this, applications would have to manage thread safety themselves.

    For example, the following code:

    ```go
    object, err := table.Get("example")
    ```

    Becomes:

    ```go
    var object []byte
    err := table.StartRead(func(tx ds.IReadTransaction) error {
        obj, err := tx.Get("example")
        if err != nil {
            return err
        }
        object = obj
        return nil
    })
    ```

# v1.7.0

**This Release Contains Breaking Changes**

- [**Breaking**] Errors have been made into constant objects. Some errors have changed. If your application relies on checking for specific errors from ds then you will need to update those checks to use `ds.Err*` constants.

# v1.6.2

- Update dependencies
- Improved tests

# v1.6.1

- Update to go 1.17

# v1.6.0

- DS tables may have unmatched indexed or unique field values [#2](https://github.com/ecnepsnai/ds/issues/2)

# v1.5.1

- Fixed a number of issues identified by static analysis

# v1.5.0

- Internal changes to support Go 1.16

# v1.4.1

- Fix a panic when calling `table.DeletePrimaryKey` or `table.DeleteUnique` with values that do not match any entries

# v1.4.0

**This Release Contains Storage Schema Changes**

- [**Schema Change**] DS Tables are now bound to a set of struct fields, rather than the name of the struct. This fixes a crash where DS would let you add a new item that had a completely different struct definition as long as the name as the same. When you register a table (including existing tables), the fields of the struct you provide are saved. When you add a new item or update an existing item, it is validated against these set of struct fields to ensure you aren't adding an incompatible object (as otherwise Gob will panic). **This update is transparent and does not require migration. However, any tables that are upgraded can not be used in older versions of DS**.
- The backup copy of migrated tables is now removed upon successful migration. This can be disabled by setting `KeepBackup` on `MigrateParams`
- Removed warning about registering anonymous table, as that is now supported
- Resolved crash when trying to close table that doesn't exist

# v1.3.0

**This Release Contains Breaking changes**

- [**Breaking**] When migrating a sorted table, the order of the table would be reversed. This is undesired behavior, but it was not documented. To avoid the possibility of somebody relying on this incorrect but undocumented behavior, we're considering this bug fix as a breaking change.

# v1.2.0

- Improved error handling for pointers
- Vastly improved documentation with examples

# v1.1.23

- Migrate to go modules

# 1.1.22

- Update logtic and bbolt
- Make tests faster

# 1.1.21

- Fix a crash when trying to sort an unsorted table

# 1.1.20

- Fixes an issue when calling `Update` with a new value on a sorted table

# 1.1.9

- Allow disabling sorting when migrating

# 1.1.8

- Add rwlock to help with concurrency

# 1.1.7

- Fix issue with unclosed tables

# 1.1.6

- Fixes update with non-existing entry

# 1.1.5

- Fix issue with maximum with GetIndex

# 1.1.4

- Fixes issue with migration
- Improved stress tool

# 1.1.3

- Replace bolt with bbolt internally

# 1.1.2

- Fixes an issue with `DeleteAll`

# 1.1.1

- Fixes an issue with `GetAll`

# 1.1.0

Added new deletion methods:

 - `DeletePrimaryKey`
 - `DeleteUnique`
 - `DeleteAll`

# 1.0.0

Initial release of DS.
