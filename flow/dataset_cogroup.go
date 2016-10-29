package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// CoGroup joins two datasets by the key,
// Each result row becomes this format:
//   (key, []left_rows, []right_rows)
func (d *Dataset) CoGroup(other *Dataset, indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	sorted_d := d.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	if d == other {
		// this should not happen, but just in case
		return sorted_d.LocalGroupBy(indexes)
	}
	sorted_other := other.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	t := sorted_d.CoGroupPartitionedSorted(sorted_other, indexes)
	t.IsLocalSorted = orderBys
	return t
}

// CoGroupPartitionedSorted joins 2 datasets that are sharded
// by the same key and already locally sorted within each shard.
func (this *Dataset) CoGroupPartitionedSorted(that *Dataset, indexes []int) (ret *Dataset) {
	ret = this.FlowContext.newNextDataset(len(this.Shards))
	ret.IsPartitionedBy = indexes

	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(instruction.NewCoGroupPartitionedSorted(indexes))
	return ret
}
