package flow

import (
	"fmt"

	"github.com/chrislusf/gleam/util"
)

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	return d.LocalSort().LocalReduce(code).MergeSortedTo(1).LocalReduce(code)
}

func (d *Dataset) LocalReduce(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalReduce"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.CreateScript()
	step.Script.Reduce(code)
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
