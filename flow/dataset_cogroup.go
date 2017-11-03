package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// CoGroup joins two datasets by the key,
// Each result row becomes this format:
//   (key, []left_rows, []right_rows)
func (d *Dataset) CoGroup(name string, other *Dataset, sortOption *SortOption) *Dataset {
	sorted_d := d.Partition(name, len(d.Shards), sortOption).LocalSort(name, sortOption)
	if d == other {
		// this should not happen, but just in case
		return sorted_d.LocalGroupBy(name, sortOption)
	}
	sorted_other := other.Partition(name, len(d.Shards), sortOption).LocalSort(name, sortOption)
	t := sorted_d.CoGroupPartitionedSorted(name, sorted_other, sortOption.Indexes())
	t.IsLocalSorted = sortOption.orderByList
	return t
}

// CoGroupPartitionedSorted joins 2 datasets that are sharded
// by the same key and already locally sorted within each shard.
func (this *Dataset) CoGroupPartitionedSorted(name string, that *Dataset, indexes []int) (ret *Dataset) {
	ret = this.Flow.NewNextDataset(len(this.Shards))
	ret.IsPartitionedBy = indexes

	inputs := []*Dataset{this, that}
	step := this.Flow.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(name, instruction.NewCoGroupPartitionedSorted(indexes))
	return ret
}
