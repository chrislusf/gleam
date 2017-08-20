package flow

import (
	"os"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/script"
)

// ReduceByKey runs the reducer registered to the reducerId,
// combining rows with the same key fields into one row
func (d *Dataset) ReduceByKey(name string, reducerId gio.ReducerId) (ret *Dataset) {
	sortOption := Field(1)
	return d.ReduceBy(name, reducerId, sortOption)
}

func (d *Dataset) ReduceBy(name string, reducerId gio.ReducerId, keyFields *SortOption) (ret *Dataset) {
	sortOption := keyFields

	name = name + ".ReduceBy"

	ret = d.LocalSort(name, sortOption).LocalReduceBy(name+".LocalReduceBy", reducerId, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(name, 1).LocalReduceBy(name+".LocalReduceBy2", reducerId, sortOption)
	}
	return ret
}

// Reduce runs the reducer registered to the reducerId,
// combining all rows into one row
func (d *Dataset) Reduce(name string, reducerId gio.ReducerId) (ret *Dataset) {

	name = name + ".Reduce"

	ret = d.LocalReduceBy(name+".LocalReduce", reducerId, nil)
	if len(d.Shards) > 1 {
		ret = ret.MergeTo(name, 1).LocalReduceBy(name+".LocalReduce2", reducerId, nil)
	}
	return ret
}

func (d *Dataset) LocalReduceBy(name string, reducerId gio.ReducerId, sortOption *SortOption) *Dataset {

	ret, step := add1ShardTo1Step(d)
	step.Name = name
	step.IsPipe = false
	step.IsGoCode = true

	// add key indexes for reducer command line option
	keyPositions := []string{}
	if sortOption != nil {
		for _, keyPosition := range sortOption.Indexes() {
			keyPositions = append(keyPositions, strconv.Itoa(keyPosition))
		}
	}
	keyFields := "0" // combine all rows directly
	if len(keyPositions) > 0 {
		keyFields = strings.Join(keyPositions, ",")
	}

	ex, _ := os.Executable()

	var args []string
	args = append(args, ex)
	args = append(args, os.Args[1:]...)
	args = append(args, "-gleam.reducer="+string(reducerId))
	args = append(args, "-gleam.keyFields="+keyFields)

	commandLine := strings.Join(args, " ")

	step.Command = script.NewShellScript().Pipe(commandLine).GetCommand()

	return ret
}
