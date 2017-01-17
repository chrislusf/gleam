package gio

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
)

type Mapper func([]interface{}) error
type Reducer func(x, y interface{}) (interface{}, error)

type gleamTaskOption struct {
	Mapper    string
	Reducer   string
	KeyFields string
}

var taskOption gleamTaskOption

func init() {
	flag.StringVar(&taskOption.Mapper, "gleam.mapper", "", "the generated mapper name")
	flag.StringVar(&taskOption.Reducer, "gleam.reducer", "", "the generated reducer name")
	flag.StringVar(&taskOption.KeyFields, "gleam.keyFields", "", "the 1-based key fields")
}

var (
	mappers  map[string]Mapper
	reducers map[string]Reducer
)

func init() {
	mappers = make(map[string]Mapper)
	reducers = make(map[string]Reducer)
}

// RegisterMapper register a mapper function to process a command
func RegisterMapper(mapperName string, fn Mapper) {
	mappers[mapperName] = fn
}

func RegisterReducer(reducerName string, fn Reducer) {
	reducers[reducerName] = fn
}

// Init determines whether the driver program will execute the mapper/reducer or not.
func Init() {
	flag.Parse()

	if taskOption.Mapper != "" || taskOption.Reducer != "" {
		runMapperReducer()
		os.Exit(0)
	}
}

// Serve starts processing stdin and writes output to stdout
func runMapperReducer() {

	if taskOption.Mapper != "" {
		if fn, ok := mappers[taskOption.Mapper]; ok {
			if err := ProcessMapper(fn); err != nil {
				log.Fatalf("Failed to execute mapper %v: %v", os.Args, err)
			}
			return
		} else {
			log.Fatalf("Failed to find mapper function for %v", taskOption.Mapper)
		}
	}

	if taskOption.Reducer != "" {
		if taskOption.KeyFields == "" {
			log.Fatalf("Also expecting values for -gleam.keyFields! Actual arguments: %v", os.Args)
		}
		if fn, ok := reducers[taskOption.Reducer]; ok {

			keyPositions := strings.Split(taskOption.KeyFields, ",")
			var keyIndexes []int
			for _, keyPosition := range keyPositions {
				keyIndex, keyIndexError := strconv.Atoi(keyPosition)
				if keyIndexError != nil {
					log.Fatalf("Failed to parse key index positions %v: %v", taskOption.KeyFields, keyIndexError)
				}
				keyIndexes = append(keyIndexes, keyIndex)
			}

			if err := ProcessReducer(fn, keyIndexes); err != nil {
				log.Fatalf("Failed to execute reducer %v: %v", os.Args, err)
			}

			return
		} else {
			log.Fatalf("Failed to find reducer function for %v", taskOption.Reducer)
		}
	}

	log.Fatalf("Failed to find function to execute. Args: %v", os.Args)
}
