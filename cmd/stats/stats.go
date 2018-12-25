package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/ecnepsnai/ds"
)

func main() {
	var tablePath string

	i := 0
	args := os.Args[1:]
	for i < len(args) {
		arg := args[i]

		if arg == "-h" || arg == "--help" {
			printHelpAndExit()
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

	err = data.View(func(tx *bolt.Tx) error {
		run(tx)
		return nil
	})
	data.Close()
	fmt.Printf("Store Size: %d\n", info.Size())
}

type bucket struct {
	Bucket *bolt.Bucket
	Name   string
}

func run(tx *bolt.Tx) {
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
	fmt.Printf("Total entries: %d\n", entryCount)

	indexCount := 0
	for _, bucket := range indexBuckets {
		bucket.Bucket.ForEach(func(k []byte, v []byte) error {
			indexCount++
			return nil
		})
	}
	fmt.Printf("Total Indexes: %d\n", indexCount)
	uniqueCount := 0
	for _, bucket := range uniqueBuckets {
		bucket.Bucket.ForEach(func(k []byte, v []byte) error {
			uniqueCount++
			return nil
		})
	}
	fmt.Printf("Total Unique Indexes: %d\n", uniqueCount)

	gob.Register(ds.Config{})
	data := configBucket.Bucket.Get([]byte("config"))
	config, err := gobDecodeConfig(data)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Name: %s\n", config.Name)
	fmt.Printf("Type: %s\n", config.TypeOf)
	fmt.Printf("Primary Key Field: %s\n", config.PrimaryKey)
	fmt.Printf("Indexed Fields: %s\n", config.Indexes)
	fmt.Printf("Unique Fields: %s\n", config.Uniques)
	fmt.Printf("Last Insert Index: %d\n", config.LastInsertIndex)
}

func printHelpAndExit() {
	fmt.Printf("Usage %s <table path>\n", os.Args[0])
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
