package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
)

var (
	InstructionRunner = &instructionRunner{}
)

type Order int

const (
	Ascending  = Order(1)
	Descending = Order(-1)
	NoOrder    = Order(0)
)

type OrderBy struct {
	Index int   // column index, starting from 1
	Order Order // Ascending or Descending
}

type Instruction interface {
	Name(string) string
	Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error
	SerializeToCommand() *pb.Instruction
	GetMemoryCostInMB(partitionSize int64) int64
}

type instructionRunner struct {
	functions []func(*pb.Instruction) Instruction
}

func (r *instructionRunner) Register(f func(*pb.Instruction) Instruction) {
	r.functions = append(r.functions, f)
}

func (r *instructionRunner) GetInstructionFunction(i *pb.Instruction) func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	for _, f := range r.functions {
		if inst := f(i); inst != nil {
			return inst.Function()
		}
	}
	return nil
}
