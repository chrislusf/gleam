package instruction

import (
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetLocalSort() != nil {
			return NewLocalSort(
				toOrderBys(m.GetLocalSort().GetOrderBys()),
				int(m.GetMemoryInMB()),
			)
		}
		return nil
	})
}

type LocalSort struct {
	orderBys   []OrderBy
	memoryInMB int
}

func NewLocalSort(orderBys []OrderBy, memoryInMB int) *LocalSort {
	return &LocalSort{orderBys, memoryInMB}
}

func (b *LocalSort) Name(prefix string) string {
	return prefix + ".LocalSort"
}

func (b *LocalSort) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoLocalSort(readers[0], writers[0], b.orderBys, stats)
	}
}

func (b *LocalSort) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalSort: &pb.Instruction_LocalSort{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalSort) GetMemoryCostInMB(partitionSize int64) int64 {
	return int64(math.Max(float64(b.memoryInMB), float64(partitionSize)))
}

func DoLocalSort(reader io.Reader, writer io.Writer, orderBys []OrderBy, stats *pb.InstructionStat) error {
	var rows []util.Row
	indexes := getIndexesFromOrderBys(orderBys)
	err := util.ProcessRow(reader, indexes, func(row util.Row) error {
		stats.InputCounter++
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		fmt.Printf("Sort>Failed to read:%v\n", err)
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(a, b int) bool {
		return lessThan(orderBys, rows[a].K, rows[b].K)
	})

	for _, row := range rows {
		// println("sorted key", kv.(pair).keys[0].(string))
		if err := row.WriteTo(writer); err != nil {
			return fmt.Errorf("Sort>Failed to write: %v", err)
		} else {
			stats.OutputCounter++
		}
	}
	return nil
}

func getIndexesFromOrderBys(orderBys []OrderBy) (indexes []int) {
	for _, o := range orderBys {
		indexes = append(indexes, o.Index)
	}
	return
}

func lessThan(orderBys []OrderBy, x, y []interface{}) bool {
	for i, order := range orderBys {
		normalOrder := order.Order >= 0
		compared := util.Compare(x[i], y[i])
		if compared < 0 {
			return normalOrder
		}
		if compared > 0 {
			return !normalOrder
		}
	}
	return false
}

func getIndexes(storedValues []int) (indexes []int32) {
	for _, x := range storedValues {
		indexes = append(indexes, int32(x))
	}
	return
}

func getOrderBys(storedValues []OrderBy) (orderBys []*pb.OrderBy) {
	for _, o := range storedValues {
		orderBys = append(orderBys, &pb.OrderBy{
			Index: int32(o.Index),
			Order: int32(o.Order),
		})
	}
	return
}
