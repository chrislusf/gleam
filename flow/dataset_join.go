package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// Join joins two datasets by the key.
func (d *Dataset) Join(name string, other *Dataset, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	return d.DoJoin(name, other, false, false, sortOption)
}

func (d *Dataset) LeftOuterJoin(name string, other *Dataset, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	return d.DoJoin(name, other, true, false, sortOption)
}

func (d *Dataset) RightOuterJoin(name string, other *Dataset, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	return d.DoJoin(name, other, false, true, sortOption)
}

func (d *Dataset) DoJoin(name string, other *Dataset, leftOuter, rightOuter bool, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	sorted_d := d.Partition(name+".left", len(d.Shards), sortOption).LocalSort(name+".left", sortOption)
	var sorted_other *Dataset
	if d == other {
		sorted_other = sorted_d
	} else {
		sorted_other = other.Partition(name+".right", len(d.Shards), sortOption).LocalSort(name+".right", sortOption)
	}
	return sorted_d.JoinPartitionedSorted(name, sorted_other, sortOption, leftOuter, rightOuter)
}

// JoinPartitionedSorted Join multiple datasets that are sharded by the same key, and locally sorted within the shard
func (this *Dataset) JoinPartitionedSorted(name string, that *Dataset, sortOption *SortOption,
	isLeftOuterJoin, isRightOuterJoin bool) *Dataset {
	ret := this.Flow.newNextDataset(len(this.Shards))
	ret.IsPartitionedBy = that.IsPartitionedBy
	ret.IsLocalSorted = that.IsLocalSorted

	inputs := []*Dataset{this, that}
	step := this.Flow.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(name, instruction.NewJoinPartitionedSorted(isLeftOuterJoin, isRightOuterJoin, sortOption.Indexes()))
	return ret
}
