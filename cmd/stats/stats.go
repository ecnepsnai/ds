package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/ecnepsnai/ds"
	"go.etcd.io/bbolt"
)

type results struct {
	EntryCount      int        `json:"entry_count"`
	IndexCount      int        `json:"index_count"`
	UnmatchedIndex  int        `json:"unmatched_index"`
	UniqueCount     int        `json:"unique_count"`
	UnmatchedUnique int        `json:"unmatched_unique"`
	Fields          []ds.Field `json:"fields"`
	TypeOf          string     `json:"type_of"`
	PrimaryKey      string     `json:"primary_key"`
	IndexedFields   []string   `json:"indexed_fields"`
	UniqueFields    []string   `json:"unique_fields"`
	LastInsertIndex uint64     `json:"last_insert_index"`
	Options         []string   `json:"options"`
	Size            int64      `json:"size"`
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

	data, err := bbolt.Open(tablePath, os.ModePerm, nil)
	if err != nil {
		panic(err)
	}

	var r results
	err = data.View(func(tx *bbolt.Tx) error {
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
	fmt.Printf("Total indexes: %d\n", r.IndexCount)
	fmt.Printf("Unmatched index values: %d\n", r.UnmatchedIndex)
	fmt.Printf("Total unique values: %d\n", r.UniqueCount)
	fmt.Printf("Unmatched unique values: %d\n", r.UnmatchedUnique)
	fmt.Printf("Name: %+v\n", r.Fields)
	fmt.Printf("Type: %s\n", r.TypeOf)
	fmt.Printf("Primary key field: %s\n", r.PrimaryKey)
	fmt.Printf("Indexed fields: %s\n", r.IndexedFields)
	fmt.Printf("Unique fields: %s\n", r.UniqueFields)
	fmt.Printf("Last insert index: %d\n", r.LastInsertIndex)
	fmt.Printf("Options: %s\n", r.Options)
	fmt.Printf("Store size: %dB\n", r.Size)
}

type bucket struct {
	Bucket *bbolt.Bucket
	Name   string
}

func run(tx *bbolt.Tx) (r results) {
	var indexBuckets []bucket
	var uniqueBuckets []bucket
	var dataBucket bucket
	var configBucket bucket

	// Find all buckets
	tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
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

	if dataBucket.Bucket == nil {
		fmt.Fprintf(os.Stderr, "Invalid input file: data bucket not found. Is this a valid DS database?\n")
		os.Exit(2)
	}
	if configBucket.Bucket == nil {
		fmt.Fprintf(os.Stderr, "Invalid input file: config bucket not found. Is this a valid DS database?\n")
		os.Exit(2)
	}

	entryCount := 0
	dataBucket.Bucket.ForEach(func(k []byte, v []byte) error {
		entryCount++
		return nil
	})
	r.EntryCount = entryCount

	indexCount := 0
	unmatchedIndexCount := 0
	for _, bucket := range indexBuckets {
		err := bucket.Bucket.ForEach(func(k []byte, v []byte) error {
			indexCount++
			keys, err := gobDecodePrimaryKeyList(v)
			if err != nil {
				return err
			}
			for _, pk := range keys {
				if tx.Bucket([]byte("data")).Get(pk) == nil {
					unmatchedIndexCount++
				}
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error iterating over table indexes: %s", err.Error())
			os.Exit(1)
		}
	}
	r.IndexCount = indexCount
	r.UnmatchedIndex = unmatchedIndexCount

	uniqueCount := 0
	unmatchedUniqueCount := 0
	for _, bucket := range uniqueBuckets {
		err := bucket.Bucket.ForEach(func(k []byte, v []byte) error {
			uniqueCount++
			if tx.Bucket([]byte("data")).Get(v) == nil {
				unmatchedUniqueCount++
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error iterating over table uniques: %s", err.Error())
			os.Exit(1)
		}
	}
	r.UniqueCount = uniqueCount
	r.UnmatchedUnique = unmatchedUniqueCount

	gob.Register(ds.Config{})
	data := configBucket.Bucket.Get([]byte("config"))
	config, err := gobDecodeConfig(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid input file: error reading config data: %s\n", err.Error())
		os.Exit(2)
	}

	optionsData := configBucket.Bucket.Get([]byte("options"))
	options, err := gobDecodeOptions(optionsData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid input file: error reading table options: %s\n", err.Error())
		os.Exit(2)
	}

	r.Fields = config.Fields
	r.TypeOf = config.TypeOf
	r.PrimaryKey = config.PrimaryKey
	r.IndexedFields = config.Indexes
	r.UniqueFields = config.Uniques
	r.LastInsertIndex = config.LastInsertIndex
	r.Options = []string{}
	if options.DisableSorting {
		r.Options = append(r.Options, "disable_sorting")
	}

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

func gobDecodeOptions(b []byte) (*ds.Options, error) {
	var w = ds.Options{}

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
