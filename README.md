# DS

Package ds (short for "data store") is a key-value store with hash indexes. It allows for rudementary but lightning fast
retrevial of grouped or relevant data without having to iterate over all objects in the store.

# Usage

## Creating A Table

Properties of your table are defined using ds struct tags. There are three tags:

 * `primary`: The primary key for each row. Value must be unique and cannot be nil. There must be exactly 1 primary key.
 * `index`: A field where other rows that share the same value are grouped togther for quick retreival.
 * `unique`: A field where the value must be unique from all other rows.

### Register your object

```golang
type exampleType struct {
    Primary string `ds:"primary"`
    Index   string `ds:"index"`
    Unique  string `ds:"unique"`
}

table, err := ds.Register(exampleType{}, "./data.db");
if err != nil {
    panic("Error registering table: %s", err.Error())
}
```

## Add something

```golang
object := exampleType{
    Primary: "foo",
    Index: "bar",
    Unique: "baz"
}

err := table.Add(object);
if err != nil {
    panic("Error adding to table: %s", err.Error())
}
```

## Get A Single Object

```golang
value, err := table.Get("foo")
if err != nil {
    panic("Error getting object: %s", err.Error())
}
if value == nil {
    // Nothing found with that primary key
}

// Now use your object
object := value.(exampleType)
```

## Get Multiple Objects

`GetIndex` returns an unsorted slice. Use `GetIndexSorted` to return based on the insertion order from
newest to oldest.

```golang
values, err := table.GetIndex("Index", "bar")
if err != nil {
    panic("Error getting many objects: %s", err.Error())
}
if len(values) == 0 {
    // Nothing found with that index value
}

// Now use your objects
for _, v := range values {
    object := value.(exampleType)
}
```