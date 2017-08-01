package flow

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/script"
)

// Mapper runs the mapper registered to the mapperId.
// This is used to execute pure Go code.
func (d *Dataset) Map(name string, mapperId gio.MapperId) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = name
	step.IsPipe = false
	step.IsGoCode = true
	var args []string
	args = append(args, "./"+filepath.Base(os.Args[0]))
	// args = append(args, os.Args[1:]...) // empty string in an arg can fail the execution
	args = append(args, "-gleam.mapper="+string(mapperId))
	commandLine := strings.Join(args, " ")
	// println("args:", commandLine)
	step.Command = script.NewShellScript().Pipe(commandLine).GetCommand()
	return ret
}

func add1ShardTo1Step(d *Dataset) (ret *Dataset, step *Step) {
	ret = d.Flow.newNextDataset(len(d.Shards))
	step = d.Flow.AddOneToOneStep(d, ret)
	return
}

// Select selects multiple fields into the next dataset. The index starts from 1.
func (d *Dataset) Select(sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)
	ret, step := add1ShardTo1Step(d)
	step.Name = "Select"
	indexes := sortOption.Indexes()
	step.SetInstruction(instruction.NewSelect([]int{indexes[0]}, indexes[1:]))
	return ret
}

// LocalLimit take the local first n rows and skip all other rows.
func (d *Dataset) LocalLimit(n int, offset int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.Name = "Limit"
	step.SetInstruction(instruction.NewLocalLimit(n, offset))
	return ret
}
