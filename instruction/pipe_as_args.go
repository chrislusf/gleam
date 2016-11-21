package instruction

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetPipeAsArgs() != nil {
			return NewPipeAsArgs(m.GetPipeAsArgs().GetCode())
		}
		return nil
	})
}

type PipeAsArgs struct {
	code string
}

func NewPipeAsArgs(code string) *PipeAsArgs {
	return &PipeAsArgs{code}
}

func (b *PipeAsArgs) Name() string {
	return "PipeAsArgs"
}

func (b *PipeAsArgs) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoPipeAsArgs(readers[0], writers[0], b.code)
	}
}

func (b *PipeAsArgs) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		PipeAsArgs: &msg.PipeAsArgs{
			Code: proto.String(b.code),
		},
	}
}

func (b *PipeAsArgs) GetMemoryCostInMB(partitionSize int) int {
	return 1
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

		command := &script.Command{
			Path: "sh",
			Args: []string{"-c", actualCode},
		}
		// write output to writer
		wg.Add(1)
		util.Execute(&wg, "PipeArgs", command.ToOsExecCommand(), nil, writer, false, true, false, os.Stderr)
		//wg.Wait()
		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "PipeArgs> Error: %v\n", err)
	}
}
