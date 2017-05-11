package instruction

import (
	"io"
	"log"

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

func (b *ScatterPartitions) Name() string {
	return "ScatterPartitions"
}

func (b *ScatterPartitions) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoScatterPartitions(readers[0], writers, b.indexes, stats)
	}
}

func (b *ScatterPartitions) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Name: b.Name(),
		ScatterPartitions: &pb.Instruction_ScatterPartitions{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *ScatterPartitions) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

func DoScatterPartitions(reader io.Reader, writers []io.Writer, indexes []int, stats *Stats) error {
	shardCount := len(writers)

	return util.ProcessMessage(reader, func(data []byte) error {
		keyObjects, err := util.DecodeRowKeys(data, indexes)
		if err != nil {
			log.Printf("Failed to find keys on %v", indexes)
			return err
		}
		stats.InputCounter++
		x := util.PartitionByKeys(shardCount, keyObjects)
		if err = util.WriteMessage(writers[x], data); err == nil {
			stats.OutputCounter++
		}
		return err
	})

}
