package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetMsgPack() != nil {
			return NewMsgPack()
		}
		return nil
	})
}

type MsgPack struct {
}

func NewMsgPack() *MsgPack {
	return &MsgPack{}
}

func (b *MsgPack) Name(prefix string) string {
	return prefix + ".MsgPack"
}

func (b *MsgPack) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoMsgPack(readers[0], writers[0], stats)
	}
}

func (b *MsgPack) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		MsgPack: &pb.Instruction_MsgPack{},
	}
}

func (b *MsgPack) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoMsgPack(reader io.Reader, writer io.Writer, stats *pb.InstructionStat) error {
	return util.TakeTsv(reader, -1, func(message []string) error {
		stats.InputCounter++
		var row []interface{}
		for _, m := range message {
			row = append(row, m)
		}
		stats.OutputCounter++
		return util.NewRow(util.Now(), row...).WriteTo(writer)
	})

}
