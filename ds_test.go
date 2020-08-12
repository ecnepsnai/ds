package ds_test

import (
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"os"
	"testing"

	"github.com/ecnepsnai/logtic"
)

var tmpDir string
var verbose bool

func isTestVerbose() bool {
	for _, arg := range os.Args {
		if arg == "-test.v=true" {
			return true
		}
	}

	return false
}

func testSetup() {
	tmp, err := ioutil.TempDir("", "certbox")
	if err != nil {
		panic(err)
	}
	tmpDir = tmp

	if verbose {
		logtic.Log.FilePath = "/dev/null"
		logtic.Log.Level = logtic.LevelDebug
		if err := logtic.Open(); err != nil {
			panic(err)
		}
	}
}

func testTeardown() {
	os.RemoveAll(tmpDir)
	logtic.Close()
}

func TestMain(m *testing.M) {
	verbose = isTestVerbose()
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
