package store_test

import (
	"fmt"
	"os"

	"github.com/ecnepsnai/ds/store"
)

func ExampleNew() {
	store, err := store.New("/path/to/your/data/dir", "StoreName", &store.Options{
		Extension: ".dat",
		Mode:      0600,
	})
	if err != nil {
		panic(err)
	}

	// Don't forget to close your store when you're finished (such as when the application exits)
	store.Close()
}

func ExampleStore_Get() {
	tmpDir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(tmpDir)
	store, err := store.New(tmpDir, "ExampleGet", nil)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	store.Write("key1", []byte("value1"))

	value := store.Get("key1")
	if value == nil {
		panic("No object with key")
	}

	fmt.Printf("Value: %s\n", value)
	// output: Value: value1
}

func ExampleStore_Count() {
	tmpDir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(tmpDir)
	store, err := store.New(tmpDir, "ExampleCount", nil)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	store.Write("key1", []byte("value1"))

	count := store.Count()
	fmt.Printf("Count: %d\n", count)
	// output: Count: 1
}

func ExampleStore_ForEach() {
	tmpDir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(tmpDir)
	store, err := store.New(tmpDir, "ExampleForEach", nil)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	store.Write("key1", []byte("value1"))
	store.Write("key2", []byte("value2"))
	store.Write("key3", []byte("value3"))

	store.ForEach(func(key string, idx int, value []byte) error {
		fmt.Printf("%s: %s\n", key, value)
		return nil
	})
	// output: key1: value1
	// key2: value2
	// key3: value3
}

func ExampleStore_Write() {
	tmpDir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(tmpDir)
	store, err := store.New(tmpDir, "ExampleWrite", nil)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	store.Write("key1", []byte("value1"))
	store.Write("key2", []byte("value2"))
}

func ExampleStore_Truncate() {
	tmpDir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(tmpDir)
	store, err := store.New(tmpDir, "ExampleTruncate", nil)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	store.Write("key1", []byte("value1"))
	fmt.Printf("Count Before: %d\n", store.Count())

	if err := store.Truncate(); err != nil {
		panic(err)
	}

	fmt.Printf("Count After: %d\n", store.Count())
	// output: Count Before: 1
	// Count After: 0
}

func ExampleStore_Delete() {
	tmpDir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(tmpDir)
	store, err := store.New(tmpDir, "ExampleDelete", nil)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	store.Write("key1", []byte("value1"))
	fmt.Printf("Value Before: %s\n", store.Get("key1"))

	store.Delete("key1")
	fmt.Printf("Value After: %s\n", store.Get("key1"))

	// output: Value Before: value1
	// Value After:
}

func ExampleStore_BeginWrite() {
	tmpDir, _ := os.MkdirTemp("", "store")
	defer os.RemoveAll(tmpDir)
	s, err := store.New(tmpDir, "ExampleBeginWrite", nil)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	err = s.BeginWrite(func(tx *store.Tx) error {
		if err := s.Write("key1", []byte("value1")); err != nil {
			return err
		}
		if err := s.Write("key2", []byte("value2")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
