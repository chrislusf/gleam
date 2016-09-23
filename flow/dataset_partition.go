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
	step.FunctionType = TypeScatterPartitions
	step.Function = func(task *Task) {
		inChan := task.InputShards[0].OutgoingChans[0]
		var outChans []chan []byte
		for _, shard := range task.OutputShards {
			outChans = append(outChans, shard.IncomingChan)
		}

		ScatterPartitions(inChan, outChans)

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
	step.FunctionType = TypeCollectPartitions
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan
		var inChans []chan []byte
		for _, shard := range task.InputShards {
			inChans = append(inChans, shard.OutgoingChans...)
		}

		CollectPartitions(inChans, outChan)

		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}
	}
	return
}

func ScatterPartitions(inChan chan []byte, outChans []chan []byte) {
	shardCount := len(outChans)

	for data := range inChan {
		keyObject, _ := util.DecodeRowKey(data)
		x := util.HashByKey(keyObject, shardCount)
		outChans[x] <- data
	}
}

func CollectPartitions(inChans []chan []byte, outChan chan []byte) {
	inputChan := make(chan []byte)
	util.MergeChannel(inChans, inputChan)
	for data := range inputChan {
		outChan <- data
	}
}
