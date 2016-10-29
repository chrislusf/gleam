package instruction

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type MergeSortedTo struct {
	orderBys []OrderBy
}

func NewMergeSortedTo(orderBys []OrderBy) *MergeSortedTo {
	return &MergeSortedTo{orderBys}
}

func (b *MergeSortedTo) Name() string {
	return "MergeSortedTo"
}

func (b *MergeSortedTo) FunctionType() FunctionType {
	return TypeMergeSortedTo
}

func (b *MergeSortedTo) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoMergeSortedTo(readers, writers[0], b.orderBys)
	}
}

func (b *MergeSortedTo) SerializeToCommand() *cmd.Instruction {
	return &cmd.Instruction{
		Name: proto.String(b.Name()),
		MergeSortedTo: &cmd.MergeSortedTo{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

// Top streamingly compare and get the top n items
func DoMergeSortedTo(readers []io.Reader, writer io.Writer, orderBys []OrderBy) {
	indexes := getIndexesFromOrderBys(orderBys)

	pq := newMinQueueOfPairs(orderBys)

	// enqueue one item to the pq from each channel
	for shardId, reader := range readers {
		if x, err := util.ReadMessage(reader); err == nil {
			if keys, err := util.DecodeRowKeys(x, indexes); err != nil {
				log.Printf("%v: %+v", err, x)
			} else {
				pq.Enqueue(pair{keys: keys, data: x}, shardId)
			}
		}
	}
	for pq.Len() > 0 {
		t, shardId := pq.Dequeue()
		util.WriteMessage(writer, t.(pair).data)
		if x, err := util.ReadMessage(readers[shardId]); err == nil {
			if keys, err := util.DecodeRowKeys(x, indexes); err != nil {
				log.Printf("%v: %+v", err, x)
			} else {
				pq.Enqueue(pair{keys: keys, data: x}, shardId)
			}
		}
	}
}
