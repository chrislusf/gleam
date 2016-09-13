package flow

import (
	"fmt"

	"github.com/chrislusf/gleam/util"
	"github.com/psilva261/timsort"
	"github.com/ugorji/go/codec"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type pair struct {
	key  interface{}
	data []byte
}

func (d *Dataset) Sort() *Dataset {
	return d.LocalSort().MergeSortedTo(1)
}

func (d *Dataset) LocalSort() *Dataset {
	if d.IsLocalSorted {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = true
	step.Name = "LocalSort"
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		var kvs []interface{}
		for input := range task.InputShards[0].OutgoingChans[0] {
			if key, err := util.DecodeRowKey(input); err != nil {
				fmt.Printf("Sort>Failed to read input data %v: %+v\n", err, input)
				break
			} else {
				kvs = append(kvs, pair{key: key, data: input})
			}
		}
		if len(kvs) == 0 {
			return
		}
		timsort.Sort(kvs, func(a, b interface{}) bool {
			x, y := a.(pair), b.(pair)
			return util.LessThan(x.key, y.key)
		})

		for _, kv := range kvs {
			outChan <- kv.(pair).data
		}

		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}
	}
	return ret
}

func (d *Dataset) MergeSortedTo(partitionCount int) (ret *Dataset) {
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
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		pq := util.NewPriorityQueue(func(a, b interface{}) bool {
			x, y := a.([]byte), b.([]byte)
			xKey, _ := util.DecodeRowKey(x)
			yKey, _ := util.DecodeRowKey(y)
			return util.LessThan(xKey, yKey)
		})
		// enqueue one item to the pq from each shard
		for shardId, shard := range task.InputShards {
			if x, ok := <-shard.OutgoingChans[0]; ok {
				pq.Enqueue(x, shardId)
			}
		}
		for pq.Len() > 0 {
			t, shardId := pq.Dequeue()
			outChan <- t.([]byte)
			if x, ok := <-task.InputShards[shardId].OutgoingChans[0]; ok {
				pq.Enqueue(x, shardId)
			}
		}
		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}

	}
	return ret
}
