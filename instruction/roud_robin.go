package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetRoundRobin() != nil {
			return NewRoundRobin()
		}
		return nil
	})
}

type RoundRobin struct {
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (b *RoundRobin) Name() string {
	return "RoundRobin"
}

func (b *RoundRobin) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoRoundRobin(readers[0], writers)
	}
}

func (b *RoundRobin) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Name:       b.Name(),
		RoundRobin: &pb.Instruction_RoundRobin{},
	}
}

func (b *RoundRobin) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoRoundRobin(reader io.Reader, writers []io.Writer) error {
	count, shardCount := 0, len(writers)
	return util.ProcessMessage(reader, func(data []byte) error {
		if count >= shardCount-1 {
			count = 0
		} else {
			count++
		}
		return util.WriteMessage(writers[count], data)
	})
}
