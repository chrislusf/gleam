package instruction

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetMergeSortedTo() != nil {
			return NewMergeSortedTo(
				toOrderBys(m.GetMergeSortedTo().GetOrderBys()),
			)
		}
		return nil
	})
}

type MergeSortedTo struct {
	orderBys []OrderBy
}

func NewMergeSortedTo(orderBys []OrderBy) *MergeSortedTo {
	return &MergeSortedTo{orderBys}
}

func (b *MergeSortedTo) Name() string {
	return "MergeSortedTo"
}

func (b *MergeSortedTo) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoMergeSortedTo(readers, writers[0], b.orderBys, stats)
	}
}

func (b *MergeSortedTo) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Name: b.Name(),
		MergeSortedTo: &pb.Instruction_MergeSortedTo{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *MergeSortedTo) GetMemoryCostInMB(partitionSize int64) int64 {
	return 20
}

func DoMergeSortedTo(readers []io.Reader, writer io.Writer, orderBys []OrderBy, stats *pb.InstructionStat) error {
	indexes := getIndexesFromOrderBys(orderBys)

	pq := newMinQueueOfPairs(orderBys)

	// enqueue one item to the pq from each channel
	for shardId, reader := range readers {
		if x, err := util.ReadMessage(reader); err == nil {
			if keys, err := util.DecodeRowKeys(x, indexes); err != nil {
				log.Printf("Failed to decode %v: %+v", err, x)
				return err
			} else {
				stats.InputCounter++
				pq.Enqueue(pair{keys: keys, data: x}, shardId)
			}
		} else {
			if err != io.EOF {
				log.Printf("DoMergeSortedTo failed start :%v", err)
				return err
			}
		}
	}
	for pq.Len() > 0 {
		t, shardId := pq.Dequeue()
		if err := util.WriteMessage(writer, t.(pair).data); err != nil {
			return err
		}
		stats.OutputCounter++

		if x, err := util.ReadMessage(readers[shardId]); err == nil {
			if keys, err := util.DecodeRowKeys(x, indexes); err != nil {
				log.Printf("Failed to decode %v: %+v", err, x)
			} else {
				stats.InputCounter++
				pq.Enqueue(pair{keys: keys, data: x}, shardId)
			}
		} else {
			if err != io.EOF {
				log.Printf("DoMergeSortedTo failed to ReadMessage :%v", err)
				return err
			}
		}
	}
	return nil
}
