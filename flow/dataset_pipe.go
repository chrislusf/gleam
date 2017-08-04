package flow

import (
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/script"
)

// Pipe runs the code as an external program, which processes the
// tab-separated input from the program's stdin, and outout to
// stdout also in tab-separated lines.
func (d *Dataset) Pipe(name, code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = name
	step.IsPipe = true
	step.Command = script.NewShellScript().Pipe(code).GetCommand()
	return ret
}

// PipeAsArgs takes each row of input, bind to variables in parameter
// code. The variables are specified via $1, $2, etc. The code is
// run as the command for an external program for each row of input.
//
// Watch for performance impact since it starts one Os process
// for each line of input.
func (d *Dataset) PipeAsArgs(name, code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.IsPipe = true
	step.SetInstruction(name, instruction.NewPipeAsArgs(code))
	return ret
}
