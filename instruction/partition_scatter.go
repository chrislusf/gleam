package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetScatterPartitions() != nil {
			return NewScatterPartitions(
				toInts(m.GetScatterPartitions().GetIndexes()),
			)
		}
		return nil
	})
}

type ScatterPartitions struct {
	indexes []int
}

func NewScatterPartitions(indexes []int) *ScatterPartitions {
	return &ScatterPartitions{indexes}
}

func (b *ScatterPartitions) Name(prefix string) string {
	return prefix + ".ScatterPartitions"
}

func (b *ScatterPartitions) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoScatterPartitions(readers[0], writers, b.indexes, stats)
	}
}

func (b *ScatterPartitions) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		ScatterPartitions: &pb.Instruction_ScatterPartitions{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *ScatterPartitions) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

func DoScatterPartitions(reader io.Reader, writers []io.Writer, indexes []int, stats *pb.InstructionStat) error {
	shardCount := len(writers)

	return util.ProcessRow(reader, indexes, func(row *util.Row) error {
		stats.InputCounter++
		x := util.PartitionByKeys(shardCount, row.K)
		if err := row.WriteTo(writers[x]); err == nil {
			stats.OutputCounter++
		}
		return nil
	})

}
