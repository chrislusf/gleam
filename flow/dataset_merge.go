package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

func (d *Dataset) MergeSortedTo(partitionCount int, sortOptions ...*SortOption) (ret *Dataset) {
	if len(d.Shards) == partitionCount {
		return d
	}
	ret = d.FlowContext.newNextDataset(partitionCount)
	everyN := len(d.Shards) / partitionCount
	if len(d.Shards)%partitionCount > 0 {
		everyN++
	}

	sortOption := concat(sortOptions)

	ret.IsLocalSorted = sortOption.orderByList
	ret.IsPartitionedBy = d.IsPartitionedBy
	step := d.FlowContext.AddLinkedNToOneStep(d, everyN, ret)
	step.SetInstruction(instruction.NewMergeSortedTo(sortOption.orderByList))
	return ret
}

func (d *Dataset) TreeMergeSortedTo(partitionCount int, factor int, sortOptions ...*SortOption) (ret *Dataset) {
	if len(d.Shards) > factor && len(d.Shards) > partitionCount {
		t := d.MergeSortedTo(len(d.Shards)/factor, sortOptions...)
		return t.TreeMergeSortedTo(partitionCount, factor, sortOptions...)
	}
	if len(d.Shards) > partitionCount {
		return d.MergeSortedTo(partitionCount, sortOptions...)
	}
	return d
}

func (d *Dataset) MergeTo(partitionCount int) (ret *Dataset) {
	if len(d.Shards) == partitionCount {
		return d
	}
	ret = d.FlowContext.newNextDataset(partitionCount)
	everyN := len(d.Shards) / partitionCount
	if len(d.Shards)%partitionCount > 0 {
		everyN++
	}

	ret.IsPartitionedBy = d.IsPartitionedBy
	step := d.FlowContext.AddLinkedNToOneStep(d, everyN, ret)
	step.SetInstruction(instruction.NewMergeTo())
	return ret
}
