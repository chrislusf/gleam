package instruction

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type PipeAsArgs struct {
	code string
}

func NewPipeAsArgs(code string) *PipeAsArgs {
	return &PipeAsArgs{code}
}

func (b *PipeAsArgs) Name() string {
	return "PipeAsArgs"
}

func (b *PipeAsArgs) FunctionType() FunctionType {
	return TypePipeAsArgs
}

func (b *PipeAsArgs) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoPipeAsArgs(readers[0], writers[0], b.code)
	}
}

func (b *PipeAsArgs) SerializeToCommand() *cmd.Instruction {
	return &cmd.Instruction{
		Name: proto.String(b.Name()),
		PipeAsArgs: &cmd.PipeAsArgs{
			Code: proto.String(b.code),
		},
	}
}

// Top streamingly compare and get the top n items
func DoPipeAsArgs(reader io.Reader, writer io.Writer, code string) {
	var wg sync.WaitGroup

	err := util.ProcessMessage(reader, func(input []byte) error {
		parts, err := util.DecodeRow(input)
		if err != nil {
			return fmt.Errorf("Failed to read input data %v: %+v\n", err, input)
		}
		// feed parts as input to the code
		actualCode := code
		for i := 1; i <= len(parts); i++ {
			var arg string
			if b, ok := parts[i-1].([]byte); ok {
				arg = string(b)
			} else {
				arg = fmt.Sprintf("%d", parts[i-1].(uint64))
			}
			actualCode = strings.Replace(actualCode, fmt.Sprintf("$%d", i), arg, -1)
		}

		// println("pipeAsArgs command:", actualCode)

		cmd := &script.Command{
			Path: "sh",
			Args: []string{"-c", actualCode},
		}
		// write output to writer
		wg.Add(1)
		util.Execute(&wg, "PipeArgs", cmd.ToOsExecCommand(), nil, writer, false, true, false, os.Stderr)
		//wg.Wait()
		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "PipeArgs> Error: %v\n", err)
	}
}
