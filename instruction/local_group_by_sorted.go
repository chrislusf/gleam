package instruction

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetLocalGroupBySorted() != nil {
			return NewLocalGroupBySorted(
				toInts(m.GetLocalGroupBySorted().GetIndexes()),
			)
		}
		return nil
	})
}

type LocalGroupBySorted struct {
	indexes []int
}

func NewLocalGroupBySorted(indexes []int) *LocalGroupBySorted {
	return &LocalGroupBySorted{indexes}
}

func (b *LocalGroupBySorted) Name(prefix string) string {
	return prefix + ".LocalGroupBySorted"
}

func (b *LocalGroupBySorted) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoLocalGroupBySorted(readers[0], writers[0], b.indexes, stats)
	}
}

func (b *LocalGroupBySorted) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalGroupBySorted: &pb.Instruction_LocalGroupBySorted{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *LocalGroupBySorted) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

func DoLocalGroupBySorted(reader io.Reader, writer io.Writer,
	indexes []int, stats *pb.InstructionStat) error {

	var prev util.Row
	err := util.ProcessRow(reader, indexes, func(row *util.Row) error {
		// write prev row if key is different
		stats.InputCounter++

		if prev.K != nil && util.Compare(row.K, prev.K) != 0 {
			if err := prev.WriteTo(writer); err != nil {
				return fmt.Errorf("Sort>Failed to write: %v", err)
			}
			stats.OutputCounter++
			prev.T, prev.K, prev.V = row.T, row.K, []interface{}{row.V}
		} else if prev.K == nil {
			prev.T, prev.K, prev.V = row.T, row.K, []interface{}{row.V}
		} else {
			prev.T = max(prev.T, row.T)
			prev.V = append(prev.V, row.V)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("LocalGroupBySorted>Failed:%v\n", err)
		return err
	}

	if err := prev.WriteTo(writer); err != nil {
		return fmt.Errorf("Sort>Failed to write: %v", err)
	}
	stats.OutputCounter++

	return nil

}
