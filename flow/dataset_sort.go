package flow

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/util"
	"github.com/psilva261/timsort"
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
	ret := d.LocalSort(indexes)
	if len(d.Shards) > 0 {
		ret = ret.MergeSortedTo(1, indexes)
	}
	return ret
}

func (d *Dataset) LocalSort(indexes []int) *Dataset {
	if intArrayEquals(d.IsLocalSorted, indexes) {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = indexes
	step.Name = "LocalSort"
	step.Params["indexes"] = indexes
	step.FunctionType = TypeLocalSort
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		LocalSort(task.InputChans[0].Reader, outChan.Writer, indexes)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}
	}
	return ret
}

func (d *Dataset) MergeSortedTo(partitionCount int, indexes []int) (ret *Dataset) {
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
	step.Params["indexes"] = indexes
	step.FunctionType = TypeMergeSortedTo
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		var inChans []io.Reader
		for _, pipe := range task.InputChans {
			inChans = append(inChans, pipe.Reader)
		}

		MergeSortedTo(inChans, outChan.Writer, indexes)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}

	}
	return ret
}

func LocalSort(inChan io.Reader, outChan io.Writer, indexes []int) {
	var kvs []interface{}
	err := util.ProcessMessage(inChan, func(input []byte) error {
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
		x, y := a.(pair), b.(pair)
		return util.LessThan(x.keys, y.keys)
	})

	for _, kv := range kvs {
		// println("sorted key", string(kv.(pair).keys[0].([]byte)))
		util.WriteMessage(outChan, kv.(pair).data)
	}
}

func MergeSortedTo(inChans []io.Reader, outChan io.Writer, indexes []int) {
	pq := util.NewPriorityQueue(func(a, b interface{}) bool {
		x, y := a.([]byte), b.([]byte)
		xKeys, _ := util.DecodeRowKeys(x, indexes)
		yKeys, _ := util.DecodeRowKeys(y, indexes)
		return util.LessThan(xKeys, yKeys)
	})
	// enqueue one item to the pq from each channel
	for shardId, shardChan := range inChans {
		if x, err := util.ReadMessage(shardChan); err == nil {
			pq.Enqueue(x, shardId)
		}
	}
	for pq.Len() > 0 {
		t, shardId := pq.Dequeue()
		util.WriteMessage(outChan, t.([]byte))
		if x, err := util.ReadMessage(inChans[shardId]); err == nil {
			pq.Enqueue(x, shardId)
		}
	}
}
