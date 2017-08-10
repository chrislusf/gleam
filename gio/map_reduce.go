package gio

import (
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/chrislusf/gleam/pb"
)

type MapperId string
type ReducerId string
type Mapper func([]interface{}) error
type Reducer func(x, y interface{}) (interface{}, error)

type gleamTaskOption struct {
	Mapper          string
	Reducer         string
	KeyFields       string
	ExecutorAddress string
	HashCode        uint
	StepId          int
	TaskId          int
	IsProfiling     bool
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
	flag.StringVar(&taskOption.ExecutorAddress, "gleam.executor", "", "executor address")
	flag.UintVar(&taskOption.HashCode, "flow.hashcode", 0, "flow hashcode")
	flag.IntVar(&taskOption.StepId, "flow.stepId", -1, "flow step id")
	flag.IntVar(&taskOption.TaskId, "flow.taskId", -1, "flow task id")
	flag.BoolVar(&taskOption.IsProfiling, "gleam.profiling", false, "profiling all steps")
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
