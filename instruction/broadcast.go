package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetBroadcast() != nil {
			return NewBroadcast()
		}
		return nil
	})
}

type Broadcast struct {
}

func NewBroadcast() *Broadcast {
	return &Broadcast{}
}

func (b *Broadcast) Name(prefix string) string {
	return prefix + ".Broadcast"
}

func (b *Broadcast) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoBroadcast(readers[0], writers, stats)
	}
}

func (b *Broadcast) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Broadcast: &pb.Instruction_Broadcast{},
	}
}

func (b *Broadcast) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoBroadcast(reader io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return util.ProcessMessage(reader, func(data []byte) error {
		stats.InputCounter++
		for _, writer := range writers {
			stats.OutputCounter++
			util.WriteMessage(writer, data)
		}
		return nil
	})
}
