package instruction

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetLocalDistinct() != nil {
			return NewLocalDistinct(
				toOrderBys(m.GetLocalDistinct().GetOrderBys()),
			)
		}
		return nil
	})
}

type LocalDistinct struct {
	orderBys []OrderBy
}

func NewLocalDistinct(orderBys []OrderBy) *LocalDistinct {
	return &LocalDistinct{orderBys}
}

func (b *LocalDistinct) Name(prefix string) string {
	return prefix + ".LocalDistinct"
}

func (b *LocalDistinct) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoLocalDistinct(readers[0], writers[0], b.orderBys, stats)
	}
}

func (b *LocalDistinct) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalDistinct: &pb.Instruction_LocalDistinct{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalDistinct) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoLocalDistinct(reader io.Reader, writer io.Writer, orderBys []OrderBy, stats *pb.InstructionStat) error {
	indexes := getIndexesFromOrderBys(orderBys)
	var prevKeys []interface{}
	return util.ProcessRow(reader, indexes, func(row *util.Row) error {
		// write the row if key is different
		stats.InputCounter++
		if prevKeys == nil || util.Compare(row.K, prevKeys) != 0 {
			if err := row.WriteTo(writer); err != nil {
				return fmt.Errorf("Sort>Failed to write: %v", err)
			}
			stats.OutputCounter++
			prevKeys = row.K
		}
		return nil
	})
}
