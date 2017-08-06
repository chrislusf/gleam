package instruction

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
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

func (b *PipeAsArgs) Name(prefix string) string {
	return prefix + ".PipeAsArgs"
}

func (b *PipeAsArgs) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoPipeAsArgs(readers[0], writers[0], b.code, stats)
	}
}

func (b *PipeAsArgs) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		PipeAsArgs: &pb.Instruction_PipeAsArgs{
			Code: b.code,
		},
	}
}

func (b *PipeAsArgs) GetMemoryCostInMB(partitionSize int64) int64 {
	return 3
}

func DoPipeAsArgs(reader io.Reader, writer io.Writer, code string, stats *pb.InstructionStat) error {
	var wg sync.WaitGroup

	err := util.ProcessRow(reader, nil, func(row *util.Row) error {
		stats.InputCounter++

		// feed parts as input to the code
		var parts []interface{}
		parts = append(parts, row.K...)
		parts = append(parts, row.V...)

		actualCode := code
		for i := 1; i <= len(parts); i++ {
			var arg string
			if b, ok := parts[i-1].(string); ok {
				arg = b
			} else if b, ok := parts[i-1].([]byte); ok {
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
		util.Execute(context.Background(), &wg, stats,
			"PipeArgs", command.ToOsExecCommand(),
			nil, writer, false, true, false,
			os.Stderr)
		//wg.Wait()
		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "PipeArgs> Error: %v\n", err)
	}
	return err
}
