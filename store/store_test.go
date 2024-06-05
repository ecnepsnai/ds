package store_test

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/ds/store"
	"go.etcd.io/bbolt"
)

func TestNew(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestNew", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()
}

func TestNewExtension(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	store, err := store.New(tmpDir, "TestNewExtension", &store.Options{
		Extension: ".dat",
	})
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	store.Close()

	if _, err := os.Stat(path.Join(tmpDir, "TestNewExtension.dat")); err != nil {
		t.Fatalf("Error stating file: %s", err.Error())
	}
}

func TestNewMode(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	store, err := store.New(tmpDir, "TestNewMode", &store.Options{
		Mode: 0600,
	})
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	store.Close()

	info, err := os.Stat(path.Join(tmpDir, "TestNewMode.db"))
	if err != nil {
		t.Fatalf("Error stating file: %s", err.Error())
	}
	if info.Mode() != 0600 {
		t.Fatalf("Incorrect file mode. Expected %d got %d", 0600, info.Mode())
	}
}

func TestWrite(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestWrite", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestGet", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	key := "hello"
	value := "world"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result := store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestUpdate", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	key := "hello"
	value := "world"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result := store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}

	value = "new"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result = store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestDelete", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	key := "hello"
	value := "world"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result := store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}

	if err := store.Delete(key); err != nil {
		t.Fatalf("Error deleting object: %s", err.Error())
	}

	result = store.Get(key)
	if result != nil {
		t.Fatalf("Unexpected value for deleted key")
	}
}

func TestCount(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestCount", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	if store.Count() != 1 {
		t.Fatalf("Incorrect object count returned")
	}
}

func TestForeach(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestForeach", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	count := 0
	store.ForEach(func(key string, idx int, value []byte) error {
		count++
		return nil
	})

	if count != 1 {
		t.Fatalf("Incorrect object count returned")
	}
}

func TestForeachError(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestForeachError", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	err = store.ForEach(func(key string, idx int, value []byte) error {
		return fmt.Errorf("boo")
	})
	if err == nil {
		t.Fatalf("No error seen when one expected")
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestTruncate", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	if store.Count() != 1 {
		t.Fatalf("Incorrect object count returned")
	}

	if err := store.Truncate(); err != nil {
		t.Fatalf("Error truncating table: %s", err.Error())
	}

	if store.Count() != 0 {
		t.Fatalf("Incorrect object count returned")
	}
}

func TestCopyTo(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestCopyTo", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	var b bytes.Buffer

	if err := store.CopyTo(&b); err != nil {
		t.Fatalf("Error copying store to writer: %s", err.Error())
	}
}

func TestBackupTo(t *testing.T) {
	t.Parallel()

	store, err := store.New(t.TempDir(), "TestBackupTo", &store.Options{
		Mode: 0600,
	})
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	backupPath := path.Join(t.TempDir(), "store.backup")
	if err := store.BackupTo(backupPath); err != nil {
		t.Fatalf("Error copying store to file: %s", err.Error())
	}

	info, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("Error stating file: %s", err.Error())
	}
	if info.Mode() != 0600 {
		t.Fatalf("Incorrect file mode. Expected %d got %d", 0600, info.Mode())
	}
}

func TestDifferentBucketName(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	store, err := store.New(tmpDir, "StoreName", &store.Options{
		BucketName: "BucketName",
	})
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	store.Close()

	db, err := bbolt.Open(path.Join(tmpDir, "StoreName.db"), 0644, nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer db.Close()
	err = db.View(func(tx *bbolt.Tx) error {
		exBucket := tx.Bucket([]byte("BucketName"))
		if exBucket == nil {
			return fmt.Errorf("expected bucket not found")
		}
		unexBucket := tx.Bucket([]byte("StoreName"))
		if unexBucket != nil {
			return fmt.Errorf("unexpected bucket found")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
}

func TestTx(t *testing.T) {
	t.Parallel()

	s, err := store.New(t.TempDir(), "TestTx", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer s.Close()

	err = s.BeginWrite(func(tx *store.Tx) error {
		if err := tx.Write("key1", []byte("value1")); err != nil {
			return err
		}

		value := tx.Get("key1")
		if string(value) != "value1" {
			return fmt.Errorf("unexpected value %s", value)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Error performing write transaction: %s", err.Error())
	}

	err = s.BeginWrite(func(tx *store.Tx) error {
		if err := tx.Delete("key1"); err != nil {
			return err
		}
		return fmt.Errorf("pass")
	})
	if err != nil && err.Error() != "pass" {
		t.Fatalf("Error performing write transaction: %s", err.Error())
	}

	if s.Get("key1") == nil {
		t.Fatalf("Failed transaction was not rolled back")
	}
}
