package flow

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/script"
)

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	ret = d.LocalReduce(code)
	if len(d.Shards) > 1 {
		sortOption := Field(1)
		ret = ret.MergeSortedTo(1, sortOption).LocalReduce(code)
		ret.IsLocalSorted = sortOption.orderByList
	}
	return ret
}

func (d *Dataset) LocalReduce(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.Name = "LocalReduce"
	step.Script = d.FlowContext.createScript()
	step.Script.Reduce(code)
	return ret
}

func (d *Dataset) ReduceBy(code string, sortOptions ...*SortOption) (ret *Dataset) {
	sortOption := concat(sortOptions)

	ret = d.LocalSort(sortOption).LocalReduceBy(code, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption).LocalReduceBy(code, sortOption)
	}
	return ret
}

// ReducerBy runs the commandLine as an external program
// The input and output are in MessagePack format.
// This is mostly used to execute external Go code.
func (d *Dataset) ReducerBy(reducerName string, sortOptions ...*SortOption) (ret *Dataset) {
	sortOption := concat(sortOptions)

	ret = d.LocalSort(sortOption).LocalReducerBy(reducerName, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption).LocalReducerBy(reducerName, sortOption)
	}
	return ret
}

func (d *Dataset) LocalReduceBy(code string, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret, step := add1ShardTo1Step(d)
	// TODO calculate IsLocalSorted IsPartitionedBy based on indexes
	step.Name = "LocalReduceBy"
	step.Script = d.FlowContext.createScript()
	step.Script.ReduceBy(code, sortOption.Indexes())
	return ret
}

func (d *Dataset) LocalReducerBy(reducerName string, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalReducerBy"
	step.IsPipe = false

	// add key indexes for reducer command line option
	keyPositions := []string{}
	for _, keyPosition := range sortOption.Indexes() {
		keyPositions = append(keyPositions, strconv.Itoa(keyPosition))
	}

	commandLine := fmt.Sprintf("./%s -gleam.reducer %s -gleam.keyFields %s",
		filepath.Base(os.Args[0]), reducerName, strings.Join(keyPositions, ","))

	step.Command = script.NewShellScript().Pipe(commandLine).GetCommand()

	return ret
}
