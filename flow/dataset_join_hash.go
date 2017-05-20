package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// HashJoin joins two datasets by putting the smaller dataset in memory on all
// executors and streams through the bigger dataset.
func (bigger *Dataset) HashJoin(smaller *Dataset, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	return smaller.Broadcast(len(bigger.Shards)).LocalHashAndJoinWith(bigger, sortOption)
}

func (this *Dataset) LocalHashAndJoinWith(that *Dataset, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := this.Flow.newNextDataset(len(that.Shards))
	ret.IsPartitionedBy = that.IsPartitionedBy
	ret.IsLocalSorted = that.IsLocalSorted
	inputs := []*Dataset{this, that}
	step := this.Flow.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(instruction.NewLocalHashAndJoinWith(sortOption.Indexes()))
	return ret
}

// Broadcast replicates itself to all shards.
func (d *Dataset) Broadcast(shardCount int) *Dataset {
	if shardCount == 1 && len(d.Shards) == shardCount {
		return d
	}
	ret := d.Flow.newNextDataset(shardCount)
	step := d.Flow.AddOneToAllStep(d, ret)
	step.SetInstruction(instruction.NewBroadcast())
	return ret
}
