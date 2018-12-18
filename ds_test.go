package ds

import (
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/logtic"
)

var tmpDir string
var verbose bool
var logFile *logtic.File

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
		l, _, err := logtic.New(path.Join(tmp, "ds.log"), logtic.LevelDebug, "")
		if err != nil {
			panic(err)
		}
		logFile = l
	}
}

func testTeardown() {
	os.RemoveAll(tmpDir)
	if logFile != nil {
		logFile.Close()
	}
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
