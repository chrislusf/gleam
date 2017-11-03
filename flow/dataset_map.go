package flow

import (
	"os"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/script"
)

// Mapper runs the mapper registered to the mapperId.
// This is used to execute pure Go code.
func (d *Dataset) Map(name string, mapperId gio.MapperId) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = name + ".Map"
	step.IsPipe = false
	step.IsGoCode = true

	ex, _ := os.Executable()

	var args []string
	args = append(args, ex)
	// args = append(args, os.Args[1:]...) // empty string in an arg can fail the execution
	args = append(args, "-gleam.mapper="+string(mapperId))
	commandLine := strings.Join(args, " ")
	// println("args:", commandLine)
	step.Command = script.NewShellScript().Pipe(commandLine).GetCommand()
	return ret
}

func add1ShardTo1Step(d *Dataset) (ret *Dataset, step *Step) {
	ret = d.Flow.NewNextDataset(len(d.Shards))
	step = d.Flow.AddOneToOneStep(d, ret)
	return
}

// Select selects multiple fields into the next dataset. The index starts from 1.
// The first one is the key
func (d *Dataset) Select(name string, sortOption *SortOption) *Dataset {
	ret, step := add1ShardTo1Step(d)
	indexes := sortOption.Indexes()
	step.SetInstruction(name, instruction.NewSelect([]int{indexes[0]}, indexes[1:]))
	return ret
}

// Select selects multiple fields into the next dataset. The index starts from 1.
func (d *Dataset) SelectKV(name string, keys, values *SortOption) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.SetInstruction(name, instruction.NewSelect(keys.Indexes(), values.Indexes()))
	return ret
}

// LocalLimit take the local first n rows and skip all other rows.
func (d *Dataset) LocalLimit(name string, n int, offset int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(name, instruction.NewLocalLimit(n, offset))
	return ret
}
