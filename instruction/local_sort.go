package instruction

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
	"github.com/psilva261/timsort"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
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

func (b *LocalSort) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoLocalSort(readers[0], writers[0], b.orderBys)
	}
}

func (b *LocalSort) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		LocalSort: &msg.LocalSort{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalSort) GetMemoryCostInMB() int {
	return b.memoryInMB
}

func DoLocalSort(reader io.Reader, writer io.Writer, orderBys []OrderBy) {
	var kvs []interface{}
	indexes := getIndexesFromOrderBys(orderBys)
	err := util.ProcessMessage(reader, func(input []byte) error {
		if keys, err := util.DecodeRowKeys(input, indexes); err != nil {
			return fmt.Errorf("%v: %+v", err, input)
		} else {
			kvs = append(kvs, pair{keys: keys, data: input})
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Sort>Failed to read input data:%v\n", err)
	}
	if len(kvs) == 0 {
		return
	}
	timsort.Sort(kvs, func(a, b interface{}) bool {
		return pairsLessThan(orderBys, a, b)
	})

	for _, kv := range kvs {
		// println("sorted key", string(kv.(pair).keys[0].([]byte)))
		util.WriteMessage(writer, kv.(pair).data)
	}
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
		if order.Order >= 0 {
			if util.LessThan(x.keys[i], y.keys[i]) {
				return true
			}
		} else {
			if !util.LessThan(x.keys[i], y.keys[i]) {
				return true
			}
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

func getOrderBys(storedValues []OrderBy) (orderBys []*msg.OrderBy) {
	for _, o := range storedValues {
		orderBys = append(orderBys, &msg.OrderBy{
			Index: proto.Int32(int32(o.Index)),
			Order: proto.Int32(int32(o.Order)),
		})
	}
	return
}
