package flow

import (
	"github.com/chrislusf/gleam/util"
)

// hash data or by data key, return a new dataset
// This is devided into 2 steps:
// 1. Each record is sharded to a local shard
// 2. The destination shard will collect its child shards and merge into one
func (d *Dataset) Partition(shard int) *Dataset {
	if d.IsKeyPartitioned && shard == len(d.Shards) {
		return d
	}
	if 1 == len(d.Shards) && shard == 1 {
		return d
	}
	ret := d.partition_scatter(shard).partition_collect(shard)
	ret.IsKeyPartitioned = true
	return ret
}

func (d *Dataset) partition_scatter(shardCount int) (ret *Dataset) {
	ret = d.FlowContext.newNextDataset(len(d.Shards) * shardCount)
	step := d.FlowContext.AddOneToEveryNStep(d, shardCount, ret)
	step.Name = "Partition_scatter"
	step.Function = func(task *Task) {
		for data := range task.MergedInputChan() {
			keyObject, _ := util.DecodeRowKey(data)
			x := util.HashByKey(keyObject, shardCount)
			task.OutputShards[x].IncomingChan <- data
		}
		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}
	}
	return
}

func (d *Dataset) partition_collect(shardCount int) (ret *Dataset) {
	ret = d.FlowContext.newNextDataset(shardCount)
	step := d.FlowContext.AddLinkedNToOneStep(d, len(d.Shards)/shardCount, ret)
	step.Name = "Partition_collect"
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan
		for data := range task.MergedInputChan() {
			outChan <- data
		}
		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}
	}
	return
}

func HashByKey(data interface{}, shardCount int) int {
	var x int
	if key, ok := data.(string); ok {
		x = int(util.Hash([]byte(key)))
	} else if key, ok := data.([]byte); ok {
		x = int(util.Hash(key))
	} else if key, ok := data.(int); ok {
		x = key
	} else if key, ok := data.(int8); ok {
		x = int(key)
	} else if key, ok := data.(int64); ok {
		x = int(key)
	} else if key, ok := data.(int32); ok {
		x = int(key)
	}
	return x % shardCount
}
