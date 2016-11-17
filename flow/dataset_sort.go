package flow

import (
	"github.com/chrislusf/gleam/instruction"
	"github.com/ugorji/go/codec"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type pair struct {
	keys []interface{}
	data []byte
}

func (d *Dataset) Sort(sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := d.LocalSort(sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption)
	}
	return ret
}

// SortBy e.g. SortBy(Field(1,2))
func (d *Dataset) SortBy(sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := d.LocalSort(sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption)
	}
	return ret
}

// Top streams through total n items, picking reverse ordered k items with O(n*log(k)) complexity.
func (d *Dataset) Top(k int, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := d.LocalTop(k, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption).LocalLimit(k)
	}
	return ret
}

func (d *Dataset) LocalSort(sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	if isOrderByEquals(d.IsLocalSorted, sortOption.orderByList) {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = sortOption.orderByList
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(instruction.NewLocalSort(sortOption.orderByList))
	return ret
}

func (d *Dataset) LocalTop(n int, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	if isOrderByExactReverse(d.IsLocalSorted, sortOption.orderByList) {
		return d.LocalLimit(n)
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = sortOption.orderByList
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(instruction.NewLocalTop(n, sortOption.orderByList))
	return ret
}

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

func isOrderByEquals(a []instruction.OrderBy, b []instruction.OrderBy) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v.Index != b[i].Index || v.Order != b[i].Order {
			return false
		}
	}
	return true
}

func isOrderByExactReverse(a []instruction.OrderBy, b []instruction.OrderBy) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v.Index != b[i].Index || v.Order == b[i].Order {
			return false
		}
	}
	return true
}

func getOrderBysFromIndexes(indexes []int) (orderBys []instruction.OrderBy) {
	for _, i := range indexes {
		orderBys = append(orderBys, instruction.OrderBy{i, instruction.Ascending})
	}
	return
}
