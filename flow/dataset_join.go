package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// Join joins two datasets by the key.
func (d *Dataset) Join(other *Dataset, indexes ...int) *Dataset {
	return d.DoJoin(other, false, false, indexes)
}

func (d *Dataset) LeftOuterJoin(other *Dataset, indexes ...int) *Dataset {
	return d.DoJoin(other, true, false, indexes)
}

func (d *Dataset) RightOuterJoin(other *Dataset, indexes ...int) *Dataset {
	return d.DoJoin(other, false, true, indexes)
}

func (d *Dataset) DoJoin(other *Dataset, leftOuter, rightOuter bool, indexes []int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	sorted_d := d.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	var sorted_other *Dataset
	if d == other {
		sorted_other = sorted_d
	} else {
		sorted_other = other.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	}
	return sorted_d.JoinPartitionedSorted(sorted_other, indexes, leftOuter, rightOuter)
}

// JoinPartitionedSorted Join multiple datasets that are sharded by the same key, and locally sorted within the shard
func (this *Dataset) JoinPartitionedSorted(that *Dataset, indexes []int,
	isLeftOuterJoin, isRightOuterJoin bool) *Dataset {
	ret := this.FlowContext.newNextDataset(len(this.Shards))
	ret.IsPartitionedBy = that.IsPartitionedBy
	ret.IsLocalSorted = that.IsLocalSorted

	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(instruction.NewJoinPartitionedSorted(isLeftOuterJoin, isRightOuterJoin, indexes))
	return ret
}
