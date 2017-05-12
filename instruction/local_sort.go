package instruction

import (
	"fmt"
	"io"
	"math"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/psilva261/timsort"
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

type pair struct {
	keys []interface{}
	data []byte
}

type LocalSort struct {
	orderBys   []OrderBy
	memoryInMB int
}

func NewLocalSort(orderBys []OrderBy, memoryInMB int) *LocalSort {
	return &LocalSort{orderBys, memoryInMB}
}

func (b *LocalSort) Name() string {
	return "LocalSort"
}

func (b *LocalSort) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoLocalSort(readers[0], writers[0], b.orderBys, stats)
	}
}

func (b *LocalSort) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Name: b.Name(),
		LocalSort: &pb.Instruction_LocalSort{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalSort) GetMemoryCostInMB(partitionSize int64) int64 {
	return int64(math.Max(float64(b.memoryInMB), float64(partitionSize)))
}

func DoLocalSort(reader io.Reader, writer io.Writer, orderBys []OrderBy, stats *pb.InstructionStat) error {
	var kvs []interface{}
	indexes := getIndexesFromOrderBys(orderBys)
	err := util.ProcessMessage(reader, func(input []byte) error {
		if keys, err := util.DecodeRowKeys(input, indexes); err != nil {
			return fmt.Errorf("%v: %+v", err, input)
		} else {
			stats.InputCounter++
			kvs = append(kvs, pair{keys: keys, data: input})
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Sort>Failed to read:%v\n", err)
		return err
	}
	if len(kvs) == 0 {
		return nil
	}
	timsort.Sort(kvs, func(a, b interface{}) bool {
		return pairsLessThan(orderBys, a, b)
	})

	for _, kv := range kvs {
		// println("sorted key", kv.(pair).keys[0].(string))
		if err := util.WriteMessage(writer, kv.(pair).data); err != nil {
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

func pairsLessThan(orderBys []OrderBy, a, b interface{}) bool {
	x, y := a.(pair), b.(pair)
	for i, order := range orderBys {
		normalOrder := order.Order >= 0
		compared := util.Compare(x.keys[i], y.keys[i])
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
