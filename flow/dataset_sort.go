package flow

import (
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/util"
	"github.com/ugorji/go/codec"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type pair struct {
	keys []interface{}
	data []byte
}

func (d *Dataset) Sort(indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	ret := d.LocalSort(orderBys)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys)
	}
	return ret
}

func (d *Dataset) SortBy(orderBys ...instruction.OrderBy) *Dataset {
	if len(orderBys) == 0 {
		orderBys = []instruction.OrderBy{instruction.OrderBy{1, instruction.Ascending}}
	}
	ret := d.LocalSort(orderBys)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys)
	}
	return ret
}

// Top streams through total n items, picking reverse ordered k items with O(n*log(k)) complexity.
func (d *Dataset) Top(k int, orderBys ...instruction.OrderBy) *Dataset {
	if len(orderBys) == 0 {
		orderBys = []instruction.OrderBy{instruction.OrderBy{1, instruction.Ascending}}
	}
	ret := d.LocalTop(k, orderBys)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys).LocalLimit(k)
	}
	return ret
}

func (d *Dataset) LocalSort(orderBys []instruction.OrderBy) *Dataset {
	if isOrderByEquals(d.IsLocalSorted, orderBys) {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = orderBys
	step.SetInstruction(instruction.NewLocalSort(orderBys))
	return ret
}

func (d *Dataset) LocalTop(n int, orderBys []instruction.OrderBy) *Dataset {
	if isOrderByExactReverse(d.IsLocalSorted, orderBys) {
		return d.LocalLimit(n)
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = orderBys
	step.SetInstruction(instruction.NewLocalTop(n, orderBys))
	return ret
}

func (d *Dataset) MergeSortedTo(partitionCount int, orderBys []instruction.OrderBy) (ret *Dataset) {
	if len(d.Shards) == partitionCount {
		return d
	}
	ret = d.FlowContext.newNextDataset(partitionCount)
	everyN := len(d.Shards) / partitionCount
	if len(d.Shards)%partitionCount > 0 {
		everyN++
	}
	step := d.FlowContext.AddLinkedNToOneStep(d, everyN, ret)
	step.SetInstruction(instruction.NewMergeSortedTo(orderBys))
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
