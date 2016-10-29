package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// HashJoin joins two datasets by putting the smaller dataset in memory on all
// executors and streams through the bigger dataset.
func (bigger *Dataset) HashJoin(smaller *Dataset, indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	return smaller.Broadcast(len(bigger.Shards)).LocalHashAndJoinWith(bigger, indexes)
}

func (this *Dataset) LocalHashAndJoinWith(that *Dataset, indexes []int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}

	ret := this.FlowContext.newNextDataset(len(that.Shards))
	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(instruction.NewLocalHashAndJoinWith(indexes))
	return ret
}

// Broadcast replicates itself in all shards.
func (d *Dataset) Broadcast(shardCount int) *Dataset {
	if shardCount == 1 && len(d.Shards) == shardCount {
		return d
	}
	ret := d.FlowContext.newNextDataset(shardCount)
	step := d.FlowContext.AddOneToAllStep(d, ret)
	step.SetInstruction(instruction.NewBroadcast())
	return ret
}
