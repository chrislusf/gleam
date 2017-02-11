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

func (b *LocalDistinct) Name() string {
	return "LocalDistinct"
}

func (b *LocalDistinct) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoLocalDistinct(readers[0], writers[0], b.orderBys)
	}
}

func (b *LocalDistinct) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Name: b.Name(),
		LocalDistinct: &pb.Instruction_LocalDistinct{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalDistinct) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoLocalDistinct(reader io.Reader, writer io.Writer, orderBys []OrderBy) error {
	indexes := getIndexesFromOrderBys(orderBys)
	var prevKeys []interface{}
	return util.ProcessMessage(reader, func(input []byte) error {
		if keys, err := util.DecodeRowKeys(input, indexes); err != nil {
			return fmt.Errorf("decode error %v: %+v", err, input)
		} else {
			if prevKeys == nil || util.Compare(keys, prevKeys) != 0 {
				if err := util.WriteRow(writer, keys...); err != nil {
					return fmt.Errorf("Sort>Failed to write: %v", err)
				}
				prevKeys = keys
			}
		}
		return nil
	})
}
