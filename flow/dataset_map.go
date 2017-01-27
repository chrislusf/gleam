package flow

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/script"
)

// Map operates on each row, and the returned results are passed to next dataset.
func (d *Dataset) Map(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Map"
	step.Script = d.FlowContext.createScript()
	step.Script.Map(code)
	return ret
}

// Mapper runs the mapper registered to the mapperId.
// This is used to execute pure Go code.
func (d *Dataset) Mapper(mapperId gio.MapperId) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Mapper"
	step.IsPipe = false
	var args []string
	args = append(args, "./"+filepath.Base(os.Args[0]))
	args = append(args, os.Args[1:]...)
	args = append(args, "-gleam.mapper")
	args = append(args, string(mapperId))
	commandLine := strings.Join(args, " ")
	step.Command = script.NewShellScript().Pipe(commandLine).GetCommand()
	return ret
}

// ForEach operates on each row, but the results are not collected.
// This is used to create some side effects.
func (d *Dataset) ForEach(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "ForEach"
	step.Script = d.FlowContext.createScript()
	step.Script.ForEach(code)
	return ret
}

// FlatMap translates each row into multiple rows.
func (d *Dataset) FlatMap(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "FlatMap"
	step.Script = d.FlowContext.createScript()
	step.Script.FlatMap(code)
	return ret
}

// Filter conditionally filter some rows into the next dataset.
// The code should be a function just returning a boolean result.
func (d *Dataset) Filter(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.Name = "Filter"
	step.Script = d.FlowContext.createScript()
	step.Script.Filter(code)
	return ret
}

func add1ShardTo1Step(d *Dataset) (ret *Dataset, step *Step) {
	ret = d.FlowContext.newNextDataset(len(d.Shards))
	step = d.FlowContext.AddOneToOneStep(d, ret)
	return
}

// Select selects multiple fields into the next dataset. The index starts from 1.
func (d *Dataset) Select(sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)
	ret, step := add1ShardTo1Step(d)
	step.Name = "Select"
	step.Script = d.FlowContext.createScript()
	step.Script.Select(sortOption.Indexes())
	return ret
}

// LocalLimit take the local first n rows and skip all other rows.
func (d *Dataset) LocalLimit(n int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.Name = "Limit"
	step.Script = d.FlowContext.createScript()
	step.Script.Limit(n)
	return ret
}
