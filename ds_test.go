package ds_test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/ecnepsnai/logtic"
)

func testSetup() {
	verbose := false
	for _, arg := range os.Args {
		if arg == "-test.v=true" {
			verbose = true
		}
	}

	if verbose {
		logtic.Log.FilePath = "/dev/null"
		logtic.Log.Level = logtic.LevelDebug
		if err := logtic.Log.Open(); err != nil {
			panic(err)
		}
	}
}

func testTeardown() {
	logtic.Log.Close()
}

func TestMain(m *testing.M) {
	testSetup()
	retCode := m.Run()
	testTeardown()
	os.Exit(retCode)
}

func randomString(length uint16) string {
	randB := make([]byte, length)
	rand.Read(randB)
	return hex.EncodeToString(randB)
}
