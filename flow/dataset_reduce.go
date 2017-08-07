package flow

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/script"
)

// ReduceBy runs the reducer registered to the reducerId,
// combining rows with the same key fields into one row
func (d *Dataset) ReduceBy(name string, reducerId gio.ReducerId, keyFields ...*SortOption) (ret *Dataset) {
	sortOption := concat(keyFields)

	ret = d.LocalSort(name, sortOption).LocalReduceBy(name+".LocalReduce", reducerId, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(name, 1, sortOption).LocalReduceBy(name+".LocalReduce2", reducerId, sortOption)
	}
	return ret
}

// Reduce runs the reducer registered to the reducerId,
// combining all rows into one row
func (d *Dataset) Reduce(name string, reducerId gio.ReducerId) (ret *Dataset) {

	ret = d.LocalReduceBy(name+".LocalReduce", reducerId)
	if len(d.Shards) > 1 {
		ret = ret.MergeTo(name, 1).LocalReduceBy(name+".LocalReduce2", reducerId)
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
	keyFields := "0" // combine all rows directly
	if len(keyPositions) > 1 {
		keyFields = strings.Join(keyPositions, ",")
	}

	var args []string
	args = append(args, "./"+filepath.Base(os.Args[0]))
	args = append(args, os.Args[1:]...)
	args = append(args, "-gleam.reducer="+string(reducerId))
	args = append(args, "-gleam.keyFields="+keyFields)

	commandLine := strings.Join(args, " ")

	step.Command = script.NewShellScript().Pipe(commandLine).GetCommand()

	return ret
}
