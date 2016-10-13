package flow

import (
	"io"

	"github.com/chrislusf/gleam/util"
)

// hash data or by data key, return a new dataset
// This is devided into 2 steps:
// 1. Each record is sharded to a local shard
// 2. The destination shard will collect its child shards and merge into one
func (d *Dataset) Partition(shard int, indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	if intArrayEquals(d.IsPartitionedBy, indexes) && shard == len(d.Shards) {
		return d
	}
	if 1 == len(d.Shards) && shard == 1 {
		return d
	}
	ret := d.partition_scatter(shard, indexes)
	if len(d.Shards) > 1 {
		ret = ret.partition_collect(shard, indexes)
	}
	ret.IsPartitionedBy = indexes
	return ret
}

func (d *Dataset) partition_scatter(shardCount int, indexes []int) (ret *Dataset) {
	ret = d.FlowContext.newNextDataset(len(d.Shards) * shardCount)
	step := d.FlowContext.AddOneToEveryNStep(d, shardCount, ret)
	step.Name = "Partition_scatter"
	step.Params["shardCount"] = shardCount
	step.Params["indexes"] = indexes
	step.FunctionType = TypeScatterPartitions
	step.Function = func(task *Task) {
		inChan := task.InputChans[0]
		var outChans []io.Writer
		for _, shard := range task.OutputShards {
			outChans = append(outChans, shard.IncomingChan.Writer)
			// println("writing to shard", shard, "channel", shard.IncomingChan, "=>", shard.OutgoingChans[0])
		}

		ScatterPartitions(inChan.Reader, outChans, indexes)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}
	}
	return
}

func (d *Dataset) partition_collect(shardCount int, indexes []int) (ret *Dataset) {
	ret = d.FlowContext.newNextDataset(shardCount)
	step := d.FlowContext.AddLinkedNToOneStep(d, len(d.Shards)/shardCount, ret)
	step.Name = "Partition_collect"
	step.FunctionType = TypeCollectPartitions
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan
		var inChans []io.Reader
		for _, out := range task.InputChans {
			// println("collect from shard", shard, "channel", out)
			inChans = append(inChans, out.Reader)
		}

		CollectPartitions(inChans, outChan.Writer)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}
	}
	return
}

func ScatterPartitions(inChan io.Reader, outChans []io.Writer, indexes []int) {
	shardCount := len(outChans)

	util.ProcessMessage(inChan, func(data []byte) error {
		keyObjects, _ := util.DecodeRowKeys(data, indexes)
		x := util.PartitionByKeys(shardCount, keyObjects)
		util.WriteMessage(outChans[x], data)
		return nil
	})
}

func CollectPartitions(inChans []io.Reader, outChan io.Writer) {
	println("starting to collect data from partitions...", len(inChans))

	if len(inChans) == 1 {
		io.Copy(outChan, inChans[0])
		return
	}

	util.CopyMultipleReaders(inChans, outChan)
}

func intArrayEquals(a []int, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
