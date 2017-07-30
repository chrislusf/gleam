package flow

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/script"
)

// ReducerBy runs the reducer registered to the reducerId.
// This is used to execute pure Go code.
func (d *Dataset) ReduceBy(name string, reducerId gio.ReducerId, sortOptions ...*SortOption) (ret *Dataset) {
	sortOption := concat(sortOptions)

	ret = d.LocalSort(sortOption).LocalReduceBy(name+".local2", reducerId, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption).LocalReduceBy(name+".local2", reducerId, sortOption)
	}
	return ret
}

func (d *Dataset) LocalReduceBy(name string, reducerId gio.ReducerId, sortOptions ...*SortOption) *Dataset {

	sortOption := concat(sortOptions)

	ret, step := add1ShardTo1Step(d)
	step.Name = name
	step.IsPipe = false
	step.IsGoCode = true

	// add key indexes for reducer command line option
	keyPositions := []string{}
	for _, keyPosition := range sortOption.Indexes() {
		keyPositions = append(keyPositions, strconv.Itoa(keyPosition))
	}

	var args []string
	args = append(args, "./"+filepath.Base(os.Args[0]))
	args = append(args, os.Args[1:]...)
	args = append(args, "-gleam.reducer="+string(reducerId))
	args = append(args, "-gleam.keyFields="+strings.Join(keyPositions, ","))
	commandLine := strings.Join(args, " ")

	step.Command = script.NewShellScript().Pipe(commandLine).GetCommand()

	return ret
}
