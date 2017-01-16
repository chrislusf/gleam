package gio

import (
	"log"
	"os"
	"strconv"
	"strings"
)

type Mapper func([]interface{}) error
type Reducer func(x, y interface{}) (interface{}, error)

var (
	mappers  map[string]Mapper
	reducers map[string]Reducer
)

func init() {
	mappers = make(map[string]Mapper)
	reducers = make(map[string]Reducer)
}

// RegisterMapper register a mapper function to process a command
func RegisterMapper(commandName string, fn Mapper) {
	mappers[commandName] = fn
}

func RegisterReducer(commandName string, fn Reducer) {
	reducers[commandName] = fn
}

// Serve starts processing stdin and writes output to stdout
func RunMapperReducer() {
	if len(os.Args) < 2 {
		log.Fatalf("Expecting one command line arguments, but got %v", os.Args)
	}

	commandName := os.Args[1]

	if fn, ok := mappers[commandName]; ok {
		if err := ProcessMapper(fn); err != nil {
			log.Fatalf("Failed to execute mapper %v: %v", os.Args, err)
		}
		return
	}

	if fn, ok := reducers[commandName]; ok {

		if len(os.Args) < 3 {
			log.Fatalf("Expecting two command line arguments, but got %v", os.Args)
		}

		keyPositionsArgument := os.Args[2]

		keyPositions := strings.Split(keyPositionsArgument, ",")
		var keyIndexes []int
		for _, keyPosition := range keyPositions {
			keyIndex, keyIndexError := strconv.Atoi(keyPosition)
			if keyIndexError != nil {
				log.Fatalf("Failed to parse key index positions %v: %v", os.Args[2], keyIndexError)
			}
			keyIndexes = append(keyIndexes, keyIndex)
		}

		if err := ProcessReducer(fn, keyIndexes); err != nil {
			log.Fatalf("Failed to execute reducer %v: %v", os.Args, err)
		}

		return
	}

	log.Fatalf("Failed to find function for %v", commandName)
}
