package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/ecnepsnai/ds"
)

type results struct {
	EntryCount      int      `json:"entry_count"`
	IndexCount      int      `json:"index_count"`
	UniqueCount     int      `json:"unique_count"`
	Name            string   `json:"name"`
	TypeOf          string   `json:"type_of"`
	PrimaryKey      string   `json:"primary_key"`
	Indexes         []string `json:"indexes"`
	Uniques         []string `json:"uniques"`
	LastInsertIndex uint64   `json:"last_insert_index"`
	Size            int64    `json:"size"`
}

func main() {
	var tablePath string
	outputJSON := false

	i := 0
	args := os.Args[1:]
	for i < len(args) {
		arg := args[i]

		if arg == "-h" || arg == "--help" {
			printHelpAndExit()
		} else if arg == "-j" || arg == "--json" {
			outputJSON = true
		} else {
			tablePath = arg
		}

		i++
	}

	if len(tablePath) == 0 {
		printHelpAndExit()
	}

	info, err := os.Stat(tablePath)
	if err != nil {
		fmt.Printf("%s file not found or accessible\n", tablePath)
		os.Exit(1)
	}

	data, err := bolt.Open(tablePath, os.ModePerm, nil)
	if err != nil {
		panic(err)
	}

	var r results
	err = data.View(func(tx *bolt.Tx) error {
		r = run(tx)
		return nil
	})
	if err != nil {
		panic(err)
	}
	data.Close()

	r.Size = info.Size()

	if outputJSON {
		data, err := json.Marshal(r)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s\n", data)
		return
	}
	fmt.Printf("Total entries: %d\n", r.EntryCount)
	fmt.Printf("Total Indexes: %d\n", r.IndexCount)
	fmt.Printf("Total Unique Indexes: %d\n", r.UniqueCount)
	fmt.Printf("Name: %s\n", r.Name)
	fmt.Printf("Type: %s\n", r.TypeOf)
	fmt.Printf("Primary Key Field: %s\n", r.PrimaryKey)
	fmt.Printf("Indexed Fields: %s\n", r.Indexes)
	fmt.Printf("Unique Fields: %s\n", r.Uniques)
	fmt.Printf("Last Insert Index: %d\n", r.LastInsertIndex)
	fmt.Printf("Store Size: %d\n", r.Size)
}

type bucket struct {
	Bucket *bolt.Bucket
	Name   string
}

func run(tx *bolt.Tx) (r results) {
	var indexBuckets []bucket
	var uniqueBuckets []bucket
	var dataBucket bucket
	var configBucket bucket

	// Find all buckets
	tx.ForEach(func(name []byte, b *bolt.Bucket) error {
		bucketName := string(name)

		if strings.HasPrefix(bucketName, "index:") {
			indexBuckets = append(indexBuckets, bucket{
				Bucket: b,
				Name:   bucketName,
			})
		} else if strings.HasPrefix(bucketName, "unique:") {
			uniqueBuckets = append(uniqueBuckets, bucket{
				Bucket: b,
				Name:   bucketName,
			})
		} else if bucketName == "data" {
			dataBucket = bucket{
				Bucket: b,
				Name:   bucketName,
			}
		} else if bucketName == "config" {
			configBucket = bucket{
				Bucket: b,
				Name:   bucketName,
			}
		}

		return nil
	})

	entryCount := 0
	dataBucket.Bucket.ForEach(func(k []byte, v []byte) error {
		entryCount++
		return nil
	})
	r.EntryCount = entryCount

	indexCount := 0
	for _, bucket := range indexBuckets {
		bucket.Bucket.ForEach(func(k []byte, v []byte) error {
			indexCount++
			return nil
		})
	}
	r.IndexCount = indexCount

	uniqueCount := 0
	for _, bucket := range uniqueBuckets {
		bucket.Bucket.ForEach(func(k []byte, v []byte) error {
			uniqueCount++
			return nil
		})
	}
	r.UniqueCount = uniqueCount

	gob.Register(ds.Config{})
	data := configBucket.Bucket.Get([]byte("config"))
	config, err := gobDecodeConfig(data)
	if err != nil {
		panic(err)
	}

	r.Name = config.Name
	r.TypeOf = config.TypeOf
	r.PrimaryKey = config.PrimaryKey
	r.Indexes = config.Indexes
	r.Uniques = config.Uniques
	r.LastInsertIndex = config.LastInsertIndex

	return
}

func printHelpAndExit() {
	fmt.Printf("Usage %s [options] <table path>\n", os.Args[0])
	fmt.Printf("Options:\n")
	fmt.Printf("-j --json  Print stats as a JSON object\n")
	fmt.Printf("-h --help  Print this help and exit\n")
	os.Exit(0)
}

func gobDecodeConfig(b []byte) (*ds.Config, error) {
	var w = ds.Config{}

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, err
	}
	return &w, nil
}

func gobDecodePrimaryKeyList(b []byte) ([][]byte, error) {
	var w [][]byte

	reader := bytes.NewReader(b)
	dec := gob.NewDecoder(reader)
	if err := dec.Decode(&w); err != nil {
		return nil, err
	}
	return w, nil
}
