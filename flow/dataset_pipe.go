package flow

import (
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/script"
)

func (d *Dataset) Pipe(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Pipe"
	step.IsPipe = true
	step.Command = script.NewShellScript().Pipe(code).GetCommand()
	return ret
}

// PipeAsArgs is similar to xargs, but simpler
func (d *Dataset) PipeAsArgs(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.IsPipe = true
	step.SetInstruction(instruction.NewPipeAsArgs(code, d.GetIsOnDiskIO()))
	return ret
}
