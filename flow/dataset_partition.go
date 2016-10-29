package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

func (d *Dataset) RoundRobin(shard int) *Dataset {
	if len(d.Shards) == shard {
		return d
	}
	ret := d.FlowContext.newNextDataset(shard)
	step := d.FlowContext.AddOneToAllStep(d, ret)
	step.SetInstruction(instruction.NewRoundRobin())
	return ret
}

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
	if shard > 1 {
		ret = ret.partition_collect(shard, indexes)
	}
	ret.IsPartitionedBy = indexes
	return ret
}

func (d *Dataset) partition_scatter(shardCount int, indexes []int) (ret *Dataset) {
	ret = d.FlowContext.newNextDataset(len(d.Shards) * shardCount)
	step := d.FlowContext.AddOneToEveryNStep(d, shardCount, ret)
	step.SetInstruction(instruction.NewScatterPartitions(indexes))
	return
}

func (d *Dataset) partition_collect(shardCount int, indexes []int) (ret *Dataset) {
	ret = d.FlowContext.newNextDataset(shardCount)
	step := d.FlowContext.AddLinkedNToOneStep(d, len(d.Shards)/shardCount, ret)
	step.SetInstruction(instruction.NewCollectPartitions())
	return
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
