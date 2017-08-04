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

func (b *RoundRobin) Name(prefix string) string {
	return prefix + ".RoundRobin"
}

func (b *RoundRobin) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoRoundRobin(readers[0], writers, stats)
	}
}

func (b *RoundRobin) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		RoundRobin: &pb.Instruction_RoundRobin{},
	}
}

func (b *RoundRobin) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoRoundRobin(reader io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	count, shardCount := 0, len(writers)
	return util.ProcessMessage(reader, func(data []byte) error {
		stats.InputCounter++
		if count >= shardCount-1 {
			count = 0
		} else {
			count++
		}
		err := util.WriteMessage(writers[count], data)
		if err == nil {
			stats.OutputCounter++
		}
		return err
	})
}
