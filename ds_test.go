package ds_test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/ecnepsnai/logtic"
)

func TestMain(m *testing.M) {
	logtic.Log.Level = logtic.LevelDebug
	if err := logtic.Log.Open(); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func randomString(length uint16) string {
	randB := make([]byte, length)
	rand.Read(randB)
	return hex.EncodeToString(randB)
}
