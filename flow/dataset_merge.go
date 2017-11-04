package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

func (d *Dataset) MergeSortedTo(name string, partitionCount int) (ret *Dataset) {
	if len(d.Shards) == partitionCount {
		return d
	}
	ret = d.Flow.NewNextDataset(partitionCount)
	everyN := len(d.Shards) / partitionCount
	if len(d.Shards)%partitionCount > 0 {
		everyN++
	}

	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step := d.Flow.AddLinkedNToOneStep(d, everyN, ret)
	step.SetInstruction(name, instruction.NewMergeSortedTo(d.IsLocalSorted))
	return ret
}

func (d *Dataset) TreeMergeSortedTo(name string, partitionCount int, factor int) (ret *Dataset) {
	if len(d.Shards) > factor && len(d.Shards) > partitionCount {
		t := d.MergeSortedTo(name, len(d.Shards)/factor)
		return t.TreeMergeSortedTo(name, partitionCount, factor)
	}
	if len(d.Shards) > partitionCount {
		return d.MergeSortedTo(name, partitionCount)
	}
	return d
}

func (d *Dataset) MergeTo(name string, partitionCount int) (ret *Dataset) {
	if len(d.Shards) == partitionCount {
		return d
	}
	ret = d.Flow.NewNextDataset(partitionCount)
	everyN := len(d.Shards) / partitionCount
	if len(d.Shards)%partitionCount > 0 {
		everyN++
	}

	ret.IsPartitionedBy = d.IsPartitionedBy
	step := d.Flow.AddLinkedNToOneStep(d, everyN, ret)
	step.SetInstruction(name, instruction.NewMergeTo())
	return ret
}
