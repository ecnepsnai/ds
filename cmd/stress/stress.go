package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/ecnepsnai/ds"
)

var workDir string
var threads int
var count int

type exampleType struct {
	Primary string `ds:"primary"`
	Index   string `ds:"index"`
	Unique  string `ds:"unique"`
	Complex complexType
}

type complexType struct {
	Nest nestType
}

type nestType struct {
	Foo string
	Bar []string
}

func main() {
	tmp, err := ioutil.TempDir("", "ds")
	if err != nil {
		panic(err)
	}

	workDir = tmp
	threads = 4
	count = 1000000000

	i := 0
	args := os.Args[1:]
	for i < len(args) {
		arg := args[i]

		if arg == "-w" || arg == "--work-dir" {
			value := args[i+1]
			workDir = value
			i++
		} else if arg == "-t" || arg == "--threads" {
			value := args[i+1]
			t, err := strconv.Atoi(value)
			if err != nil {
				panic(err)
			}
			threads = t
			i++
		} else if arg == "-n" || arg == "--count" {
			value := args[i+1]
			t, err := strconv.Atoi(value)
			if err != nil {
				panic(err)
			}
			count = t
			i++
		} else if arg == "-h" || arg == "--help" {
			fmt.Printf("Usage %s [options]\n", os.Args[0])
			fmt.Printf("Options:\n")
			fmt.Printf(" -w --work-dir\tWork directory where database files are outputted. Defaults to temporary directory.\n")
			fmt.Printf(" -t --threads\tNumber of threads. Defaults to 4.\n")
			fmt.Printf(" -n --count\tNumber of items to insert. Defaults to 1 billion.\n")
			os.Exit(0)
		}

		i++
	}

	tablePath := path.Join(workDir, randomString(6)+".db")
	fmt.Printf("table_path='%s' threads=%d count=%d\n", tablePath, threads, count)

	table, err := ds.Register(exampleType{}, tablePath, nil)
	if err != nil {
		panic(fmt.Sprintf("Error registering table: %s", err.Error()))
	}

	wg := sync.WaitGroup{}
	wg.Add(threads)

	y := 0
	for y < threads {
		go stress(table, &wg)
		y++
	}

	wg.Wait()

	os.RemoveAll(tmp)
}

func stress(table *ds.Table, wg *sync.WaitGroup) {
	defer wg.Done()
	i := 0

	index := randomString(258)
	var lastInserted *exampleType

	for i < count {
		i++
		action := randomNumber(1, 5)
		if action == 1 {
			// Add uique
			o := exampleType{
				Primary: randomString(258),
				Index:   randomString(258),
				Unique:  randomString(258),
				Complex: complexType{
					Nest: nestType{
						Foo: randomString(258),
						Bar: []string{
							randomString(258),
						},
					},
				},
			}
			lastInserted = &o
			if err := table.Add(o); err != nil {
				panic(fmt.Sprintf("Error adding value to table: %s", err.Error()))
			}
		} else if action == 2 {
			// Delete Last
			if lastInserted == nil {
				continue
			}

			if err := table.Delete(*lastInserted); err != nil {
				panic(fmt.Sprintf("Error deleting value to table: %s", err.Error()))
			}
			lastInserted = nil
		} else if action == 3 {
			// Add Index
			o := exampleType{
				Primary: randomString(258),
				Index:   index,
				Unique:  randomString(258),
				Complex: complexType{
					Nest: nestType{
						Foo: randomString(258),
						Bar: []string{
							randomString(258),
						},
					},
				},
			}
			lastInserted = &o
			if err := table.Add(o); err != nil {
				panic(fmt.Sprintf("Error adding value to table: %s", err.Error()))
			}
		} else if action == 4 {
			// Update Last
			if lastInserted == nil {
				continue
			}

			lastInserted.Unique = randomString(258)
			if err := table.Update(*lastInserted); err != nil {
				panic(fmt.Sprintf("Error updating value to table: %s", err.Error()))
			}
		} else if action == 5 {
			// Add large
			o := exampleType{
				Primary: randomString(uint16(randomNumber(1000, 9999))),
				Index:   randomString(uint16(randomNumber(1000, 9999))),
				Unique:  randomString(uint16(randomNumber(1000, 9999))),
				Complex: complexType{
					Nest: nestType{
						Foo: randomString(uint16(randomNumber(1000, 9999))),
						Bar: []string{
							randomString(uint16(randomNumber(1000, 9999))),
						},
					},
				},
			}
			lastInserted = &o
			if err := table.Add(o); err != nil {
				panic(fmt.Sprintf("Error adding value to table: %s", err.Error()))
			}
		}
	}
}

func randomNumber(min, max int) int {
	return mrand.Intn(max-min) + min
}

func randomString(length uint16) string {
	randB := make([]byte, length)
	rand.Read(randB)
	return hex.EncodeToString(randB)
}
