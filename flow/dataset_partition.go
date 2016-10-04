package flow

import (
	"io"

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
	ret := d.partition_scatter(shard)
	if len(d.Shards) > 1 {
		ret = ret.partition_collect(shard)
	}
	ret.IsKeyPartitioned = true
	return ret
}

func (d *Dataset) partition_scatter(shardCount int) (ret *Dataset) {
	ret = d.FlowContext.newNextDataset(len(d.Shards) * shardCount)
	step := d.FlowContext.AddOneToEveryNStep(d, shardCount, ret)
	step.Name = "Partition_scatter"
	step.Params["shardCount"] = shardCount
	step.FunctionType = TypeScatterPartitions
	step.Function = func(task *Task) {
		inChan := task.InputShards[0].OutgoingChans[0]
		var outChans []io.Writer
		for _, shard := range task.OutputShards {
			outChans = append(outChans, shard.IncomingChan.Writer)
			// println("writing to shard", shard, "channel", shard.IncomingChan, "=>", shard.OutgoingChans[0])
		}

		ScatterPartitions(inChan.Reader, outChans)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
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
		var inChans []io.Reader
		for _, shard := range task.InputShards {
			for _, out := range shard.OutgoingChans {
				// println("collect from shard", shard, "channel", out)
				inChans = append(inChans, out.Reader)
			}
		}

		CollectPartitions(inChans, outChan.Writer)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}
	}
	return
}

func ScatterPartitions(inChan io.Reader, outChans []io.Writer) {
	shardCount := len(outChans)

	util.ProcessMessage(inChan, func(data []byte) error {
		keyObject, _ := util.DecodeRowKey(data)
		x := util.HashByKey(keyObject, shardCount)
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
