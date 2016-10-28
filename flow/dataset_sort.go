package flow

import (
	"container/heap"
	"fmt"
	"io"
	"log"

	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/util"
	"github.com/ugorji/go/codec"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type pair struct {
	keys []interface{}
	data []byte
}

func (d *Dataset) Sort(indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	ret := d.LocalSort(orderBys)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys)
	}
	return ret
}

func (d *Dataset) SortBy(orderBys ...instruction.OrderBy) *Dataset {
	if len(orderBys) == 0 {
		orderBys = []instruction.OrderBy{instruction.OrderBy{1, instruction.Ascending}}
	}
	ret := d.LocalSort(orderBys)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys)
	}
	return ret
}

// Top streams through total n items, picking reverse ordered k items with O(n*log(k)) complexity.
func (d *Dataset) Top(k int, orderBys ...instruction.OrderBy) *Dataset {
	if len(orderBys) == 0 {
		orderBys = []instruction.OrderBy{instruction.OrderBy{1, instruction.Ascending}}
	}
	ret := d.LocalTop(k, orderBys)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys).LocalLimit(k)
	}
	return ret
}

func (d *Dataset) LocalSort(orderBys []instruction.OrderBy) *Dataset {
	if isOrderByEquals(d.IsLocalSorted, orderBys) {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = orderBys
	step.SetInstruction(instruction.NewLocalSort(orderBys))
	return ret
}

func (d *Dataset) LocalTop(n int, orderBys []instruction.OrderBy) *Dataset {
	if isOrderByExactReverse(d.IsLocalSorted, orderBys) {
		return d.LocalLimit(n)
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = orderBys
	step.Name = "LocalTop"
	step.Params["n"] = n
	step.Params["orderBys"] = orderBys
	step.FunctionType = instruction.TypeLocalTop
	step.Function = func(readers []io.Reader, writers []io.Writer, stats *instruction.Stats) {
		LocalTop(readers[0], writers[0], n, orderBys)
	}
	return ret
}

func (d *Dataset) MergeSortedTo(partitionCount int, orderBys []instruction.OrderBy) (ret *Dataset) {
	if len(d.Shards) == partitionCount {
		return d
	}
	ret = d.FlowContext.newNextDataset(partitionCount)
	everyN := len(d.Shards) / partitionCount
	if len(d.Shards)%partitionCount > 0 {
		everyN++
	}
	step := d.FlowContext.AddLinkedNToOneStep(d, everyN, ret)
	step.Name = fmt.Sprintf("MergeSortedTo %d", partitionCount)
	step.Params["orderBys"] = orderBys
	step.FunctionType = instruction.TypeMergeSortedTo
	step.Function = func(readers []io.Reader, writers []io.Writer, stats *instruction.Stats) {
		MergeSortedTo(readers, writers[0], orderBys)
	}
	return ret
}

func MergeSortedTo(readers []io.Reader, writer io.Writer, orderBys []instruction.OrderBy) {
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

// Top streamingly compare and get the top n items
func LocalTop(reader io.Reader, writer io.Writer, n int, orderBys []instruction.OrderBy) {
	indexes := getIndexesFromOrderBys(orderBys)
	pq := newMinQueueOfPairs(orderBys)

	err := util.ProcessMessage(reader, func(input []byte) error {
		if keys, err := util.DecodeRowKeys(input, indexes); err != nil {
			return fmt.Errorf("%v: %+v", err, input)
		} else {
			if pq.Len() >= n {
				heap.Pop(pq)
			}
			pq.Enqueue(pair{keys: keys, data: input}, 0)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Top>Failed to process input data:%v\n", err)
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
}

func isOrderByEquals(a []instruction.OrderBy, b []instruction.OrderBy) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v.Index != b[i].Index || v.Order != b[i].Order {
			return false
		}
	}
	return true
}

func isOrderByExactReverse(a []instruction.OrderBy, b []instruction.OrderBy) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v.Index != b[i].Index || v.Order == b[i].Order {
			return false
		}
	}
	return true
}

func getIndexesFromOrderBys(orderBys []instruction.OrderBy) (indexes []int) {
	for _, o := range orderBys {
		indexes = append(indexes, o.Index)
	}
	return
}

func getOrderBysFromIndexes(indexes []int) (orderBys []instruction.OrderBy) {
	for _, i := range indexes {
		orderBys = append(orderBys, instruction.OrderBy{i, instruction.Ascending})
	}
	return
}

func newMinQueueOfPairs(orderBys []instruction.OrderBy) *util.PriorityQueue {
	return util.NewPriorityQueue(func(a, b interface{}) bool {
		return pairsLessThan(orderBys, a, b)
	})
}

func pairsLessThan(orderBys []instruction.OrderBy, a, b interface{}) bool {
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
