package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/iamBelugaa/kvix/pkg/errors"
	"github.com/iamBelugaa/kvix/pkg/kvix"
)

func main() {
	cache, err := kvix.NewInstance(context.Background(), "kvix")
	if err != nil {
		if err, ok := errors.AsStorageError(err); ok {
			log.Printf("Code: %#v \n", err.Code())
			log.Printf("Details: %#v \n", err.Details())
			log.Printf("Error: %#v \n", err.Error())
			log.Printf("FileName: %#v \n", err.FileName())
			log.Printf("Offset: %#v \n", err.Offset())
			log.Printf("Path: %#v \n", err.Path())
			log.Printf("SegmentId: %#v \n", err.SegmentId())
		}
	}

	defer func() {
		if err := cache.Close(); err != nil {
			log.Fatalf("instance close error : %#v \n", err)
		}
	}()

	key := []byte("user:123")
	value := []byte("This is some personal data")

	if err := cache.Set(context.Background(), key, value); err != nil {
		log.Fatalf("set operation error : %#v \n", err)
	}

	record, err := cache.Get(context.Background(), key)
	if err != nil {
		if err, ok := errors.AsStorageError(err); ok {
			log.Printf("Code: %#v \n", err.Code())
			log.Printf("Details: %#v \n", err.Details())
			log.Printf("Error: %#v \n", err.Error())
			log.Printf("FileName: %#v \n", err.FileName())
			log.Printf("Offset: %#v \n", err.Offset())
			log.Printf("Path: %#v \n", err.Path())
			log.Printf("SegmentId: %#v \n", err.SegmentId())
		}
	}

	jsonData, _ := json.MarshalIndent(record, "", "  ")
	println(string(jsonData))
}
