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

func (b *MergeSortedTo) Name(prefix string) string {
	return prefix + ".MergeSortedTo"
}

func (b *MergeSortedTo) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoMergeSortedTo(readers, writers[0], b.orderBys, stats)
	}
}

func (b *MergeSortedTo) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		MergeSortedTo: &pb.Instruction_MergeSortedTo{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *MergeSortedTo) GetMemoryCostInMB(partitionSize int64) int64 {
	return 20
}

type rowWithOriginalData struct {
	row       *util.Row
	originalK []interface{}
	originalV []interface{}
}

func newRowWithOriginalData(row *util.Row) *rowWithOriginalData {
	return &rowWithOriginalData{row: row, originalK: row.K, originalV: row.V}
}

func newMinQueueOfRowsWithOriginalData(orderBys []OrderBy) *util.PriorityQueue {
	return util.NewPriorityQueue(func(a, b interface{}) bool {
		x, y := a.(*rowWithOriginalData), b.(*rowWithOriginalData)
		return lessThan(orderBys, x.row, y.row)
	})
}

func DoMergeSortedTo(readers []io.Reader, writer io.Writer, orderBys []OrderBy, stats *pb.InstructionStat) error {
	indexes := getIndexesFromOrderBys(orderBys)

	pq := newMinQueueOfRowsWithOriginalData(orderBys)

	// enqueue one item to the pq from each channel
	for shardId, reader := range readers {
		if row, err := util.ReadRow(reader); err == nil {
			rowWithOriginalData := newRowWithOriginalData(row)
			row.UseKeys(indexes)
			stats.InputCounter++
			pq.Enqueue(rowWithOriginalData, shardId)
		} else {
			if err != io.EOF {
				log.Printf("DoMergeSortedTo failed start :%v", err)
				return err
			}
		}
	}
	for pq.Len() > 0 {
		t, shardId := pq.Dequeue()
		rowWithOriginalData := t.(*rowWithOriginalData)
		rowWithOriginalData.row.K = rowWithOriginalData.originalK
		rowWithOriginalData.row.V = rowWithOriginalData.originalV
		if err := rowWithOriginalData.row.WriteTo(writer); err != nil {
			return err
		}
		stats.OutputCounter++

		if row, err := util.ReadRow(readers[shardId]); err == nil {
			rowWithOriginalData := newRowWithOriginalData(row)
			row.UseKeys(indexes)
			stats.InputCounter++
			pq.Enqueue(rowWithOriginalData, shardId)
		} else {
			if err != io.EOF {
				log.Printf("DoMergeSortedTo failed to ReadRow :%v", err)
				return err
			}
		}
	}
	return nil
}
