package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetLocalLimit() != nil {
			return NewLocalLimit(
				int(m.GetLocalLimit().GetN()),
				int(m.GetLocalLimit().GetOffset()),
			)
		}
		return nil
	})
}

type LocalLimit struct {
	n      int
	offset int
}

func NewLocalLimit(n int, offset int) *LocalLimit {
	return &LocalLimit{n, offset}
}

func (b *LocalLimit) Name(prefix string) string {
	return prefix + ".LocalLimit"
}

func (b *LocalLimit) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoLocalLimit(readers[0], writers[0], b.n, b.offset, stats)
	}
}

func (b *LocalLimit) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalLimit: &pb.Instruction_LocalLimit{
			N:      int32(b.n),
			Offset: int32(b.offset),
		},
	}
}

func (b *LocalLimit) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

// DoLocalLimit streamingly get the n items starting from offset
func DoLocalLimit(reader io.Reader, writer io.Writer, n int, offset int, stats *pb.InstructionStat) error {

	return util.ProcessRow(reader, nil, func(row *util.Row) error {
		stats.InputCounter++

		if offset > 0 {
			offset--
		} else {
			if n > 0 {
				row.WriteTo(writer)
				stats.OutputCounter++
			}
			n--
		}

		return nil
	})

}
