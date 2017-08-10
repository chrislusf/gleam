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

// Distinct sort on specific fields and pick the unique ones.
// Required Memory: about same size as each partition.
// example usage: Distinct(Field(1,2)) means
// distinct on field 1 and 2.
// TODO: optimize for low cardinality case.
func (d *Dataset) Distinct(name string, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := d.LocalSort(name, sortOption).LocalDistinct(name, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(name, 1, sortOption).LocalDistinct(name, sortOption)
	}
	return ret
}

// Sort sort on specific fields, default to the first field.
// Required Memory: about same size as each partition.
// example usage: Sort(Field(1,2)) means
// sorting on field 1 and 2.
func (d *Dataset) Sort(name string, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := d.LocalSort(name, sortOption)
	ret = ret.TreeMergeSortedTo(name, 1, 10, sortOption)
	return ret
}

// Top streams through total n items, picking reverse ordered k items with O(n*log(k)) complexity.
// Required Memory: about same size as n items in memory
func (d *Dataset) Top(name string, k int, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := d.LocalTop(name, k, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(name, 1, sortOption).LocalLimit(name, k, 0)
	}
	return ret
}

func (d *Dataset) LocalDistinct(name string, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = sortOption.orderByList
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(name, instruction.NewLocalDistinct(sortOption.orderByList))
	return ret
}

func (d *Dataset) LocalSort(name string, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	if isOrderByEquals(d.IsLocalSorted, sortOption.orderByList) {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = sortOption.orderByList
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(name, instruction.NewLocalSort(sortOption.orderByList, int(d.GetPartitionSize())*3))
	return ret
}

func (d *Dataset) LocalTop(name string, n int, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = getReverseOrderBy(sortOption.orderByList)
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(name, instruction.NewLocalTop(n, ret.IsLocalSorted))

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

func getReverseOrderBy(a []instruction.OrderBy) (reversed []instruction.OrderBy) {
	for _, v := range a {
		if v.Order == instruction.Ascending {
			reversed = append(reversed, instruction.OrderBy{v.Index, instruction.Descending})
		} else {
			reversed = append(reversed, instruction.OrderBy{v.Index, instruction.Ascending})
		}
	}
	return reversed
}

func getOrderBysFromIndexes(indexes []int) (orderBys []instruction.OrderBy) {
	for _, i := range indexes {
		orderBys = append(orderBys, instruction.OrderBy{i, instruction.Ascending})
	}
	return
}
