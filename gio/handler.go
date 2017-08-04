package gio

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/chrislusf/gleam/pb"
)

type MapperId string
type ReducerId string
type Mapper func([]interface{}) error
type Reducer func(x, y interface{}) (interface{}, error)

type gleamTaskOption struct {
	Mapper       string
	Reducer      string
	KeyFields    string
	AgentAddress string
	HashCode     uint
	StepId       int
	TaskId       int
}

type gleamRunner struct {
	Option *gleamTaskOption
}

var (
	HasInitalized bool

	taskOption gleamTaskOption
	stat       = &pb.ExecutionStat{} // TsEmit() needs this global value
)

func init() {
	flag.StringVar(&taskOption.Mapper, "gleam.mapper", "", "the generated mapper name")
	flag.StringVar(&taskOption.Reducer, "gleam.reducer", "", "the generated reducer name")
	flag.StringVar(&taskOption.KeyFields, "gleam.keyFields", "", "the 1-based key fields")
	flag.StringVar(&taskOption.AgentAddress, "gleam.agent", "", "agent address")
	flag.UintVar(&taskOption.HashCode, "flow.hashcode", 0, "flow hashcode")
	flag.IntVar(&taskOption.StepId, "flow.stepId", -1, "flow step id")
	flag.IntVar(&taskOption.TaskId, "flow.taskId", -1, "flow task id")
}

var (
	mappers      map[string]Mapper
	reducers     map[string]Reducer
	mappersLock  sync.Mutex
	reducersLock sync.Mutex
)

func init() {
	mappers = make(map[string]Mapper)
	reducers = make(map[string]Reducer)
}

// RegisterMapper register a mapper function to process a command
func RegisterMapper(fn Mapper) MapperId {
	mappersLock.Lock()
	defer mappersLock.Unlock()

	mapperName := fmt.Sprintf("m%d", len(mappers)+1)
	mappers[mapperName] = fn
	return MapperId(mapperName)
}

func RegisterReducer(fn Reducer) ReducerId {
	reducersLock.Lock()
	defer reducersLock.Unlock()

	reducerName := fmt.Sprintf("r%d", len(reducers)+1)
	reducers[reducerName] = fn
	return ReducerId(reducerName)
}

// Init determines whether the driver program will execute the mapper/reducer or not.
// If the command line invokes the mapper or reducer, execute it and exit.
// This function will invoke flag.Parse() first.
func Init() {
	HasInitalized = true

	flag.Parse()

	if taskOption.Mapper != "" || taskOption.Reducer != "" {
		runner := &gleamRunner{Option: &taskOption}
		runner.runMapperReducer()
		os.Exit(0)
	}
}

// Serve starts processing stdin and writes output to stdout
func (runner *gleamRunner) runMapperReducer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stat.FlowHashCode = uint32(runner.Option.HashCode)
	stat.Stats = []*pb.InstructionStat{
		&pb.InstructionStat{
			StepId: int32(runner.Option.StepId),
			TaskId: int32(runner.Option.TaskId),
		},
	}

	if runner.Option.Mapper != "" {
		if fn, ok := mappers[runner.Option.Mapper]; ok {
			if err := runner.processMapper(ctx, fn); err != nil {
				log.Fatalf("Failed to execute mapper %v: %v", os.Args, err)
			}
			return
		} else {
			log.Fatalf("Failed to find mapper function for %v", runner.Option.Mapper)
		}
	}

	if runner.Option.Reducer != "" {
		if runner.Option.KeyFields == "" {
			log.Fatalf("Also expecting values for -gleam.keyFields! Actual arguments: %v", os.Args)
		}
		if fn, ok := reducers[runner.Option.Reducer]; ok {

			keyPositions := strings.Split(runner.Option.KeyFields, ",")
			var keyIndexes []int
			for _, keyPosition := range keyPositions {
				keyIndex, keyIndexError := strconv.Atoi(keyPosition)
				if keyIndexError != nil {
					log.Fatalf("Failed to parse key index positions %v: %v", runner.Option.KeyFields, keyIndexError)
				}
				keyIndexes = append(keyIndexes, keyIndex)
			}

			if err := runner.processReducer(ctx, fn, keyIndexes); err != nil {
				log.Fatalf("Failed to execute reducer %v: %v", os.Args, err)
			}

			return
		} else {
			log.Fatalf("Failed to find reducer function for %v", runner.Option.Reducer)
		}
	}

	log.Fatalf("Failed to find function to execute. Args: %v", os.Args)
}
