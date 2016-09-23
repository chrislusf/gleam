package flow

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
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
	step.Name = "PipeArgs"
	step.IsPipe = true
	step.FunctionType = TypePipeAsArgs
	step.Params["code"] = code
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		inChan := task.InputShards[0].OutgoingChans[0]

		PipeAsArgs(inChan, code, outChan)

		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}
	}
	return ret
}

func PipeAsArgs(inChan chan []byte, code string, outChan chan []byte) {
	var wg sync.WaitGroup

	for input := range inChan {
		parts, err := util.DecodeRow(input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "PipeArgs>Failed to read input data %v: %+v\n", err, input)
			break
		}
		// feed parts as input to the code
		actualCode := code
		for i := 1; i <= len(parts); i++ {
			arg := string(parts[i-1].([]byte))
			actualCode = strings.Replace(actualCode, fmt.Sprintf("$%d", i), arg, -1)
		}

		cmd := &script.Command{
			Path: "sh",
			Args: []string{"-c", actualCode},
		}
		// write output to outChan
		wg.Add(1)
		util.Execute(&wg, "PipeArgs", cmd.ToOsExecCommand(), nil, outChan, true, false, os.Stderr)
		wg.Wait()
	}
}
