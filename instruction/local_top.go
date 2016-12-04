package instruction

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetLocalTop() != nil {
			return NewLocalTop(
				int(m.GetLocalTop().GetN()),
				toOrderBys(m.GetLocalTop().GetOrderBys()),
			)
		}
		return nil
	})
}

type LocalTop struct {
	n        int
	orderBys []OrderBy
}

func NewLocalTop(n int, orderBys []OrderBy) *LocalTop {
	return &LocalTop{n, orderBys}
}

func (b *LocalTop) Name() string {
	return "LocalTop"
}

func (b *LocalTop) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoLocalTop(readers[0], writers[0], b.n, b.orderBys)
	}
}

func (b *LocalTop) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		LocalTop: &msg.LocalTop{
			N:        proto.Int32(int32(b.n)),
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalTop) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

// Top streamingly compare and get the top n items
func DoLocalTop(reader io.Reader, writer io.Writer, n int, orderBys []OrderBy) error {
	indexes := getIndexesFromOrderBys(orderBys)
	pq := newMinQueueOfPairs(orderBys)

	err := util.ProcessMessage(reader, func(input []byte) error {
		if keys, err := util.DecodeRowKeys(input, indexes); err != nil {
			return fmt.Errorf("%v: %+v", err, input)
		} else {
			newPair := pair{keys: keys, data: input}
			if pq.Len() >= n {
				if pairsLessThan(orderBys, pq.Top(), newPair) {
					pq.Dequeue()
					pq.Enqueue(newPair, 0)
				}
			} else {
				pq.Enqueue(newPair, 0)

			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Top>Failed to process input data:%v\n", err)
		return err
	}

	// read data out of the priority queue
	length := pq.Len()
	itemsToReverse := make([][]byte, length)
	for i := 0; i < length; i++ {
		kv, _ := pq.Dequeue()
		itemsToReverse[i] = kv.(pair).data
	}
	for i := length - 1; i >= 0; i-- {
		util.WriteMessage(writer, itemsToReverse[i])
	}

	return nil
}

func newMinQueueOfPairs(orderBys []OrderBy) *util.PriorityQueue {
	return util.NewPriorityQueue(func(a, b interface{}) bool {
		return pairsLessThan(orderBys, a, b)
	})
}
