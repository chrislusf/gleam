package instruction

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
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

func (b *LocalTop) Name(prefix string) string {
	return prefix + ".LocalTop"
}

func (b *LocalTop) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoLocalTop(readers[0], writers[0], b.n, b.orderBys, stats)
	}
}

func (b *LocalTop) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalTop: &pb.Instruction_LocalTop{
			N:        int32(b.n),
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalTop) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

// DoLocalTop streamingly compare and get the top n items
func DoLocalTop(reader io.Reader, writer io.Writer, n int, orderBys []OrderBy, stats *pb.InstructionStat) error {

	pq := newMinQueueOfPairs(orderBys)

	err := util.ProcessRow(reader, nil, func(row *util.Row) error {
		stats.InputCounter++

		if pq.Len() >= n {
			if lessThan(orderBys, pq.Top().(*util.Row), row) {
				pq.Dequeue()
				pq.Enqueue(row, 0)
			}
		} else {
			pq.Enqueue(row, 0)

		}
		return nil
	})
	if err != nil {
		fmt.Printf("Top>Failed to process input data:%v\n", err)
		return err
	}

	// read data out of the priority queue
	length := pq.Len()
	itemsToReverse := make([]*util.Row, length)
	for i := 0; i < length; i++ {
		entry, _ := pq.Dequeue()
		itemsToReverse[i] = entry.(*util.Row)
	}
	for i := length - 1; i >= 0; i-- {
		itemsToReverse[i].WriteTo(writer)
		stats.OutputCounter++
	}

	return nil
}

func newMinQueueOfPairs(orderBys []OrderBy) *util.PriorityQueue {
	return util.NewPriorityQueue(func(a, b interface{}) bool {
		x, y := a.(*util.Row), b.(*util.Row)
		return lessThan(orderBys, x, y)
	})
}
