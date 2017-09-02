package flow

import (
	"github.com/chrislusf/gleam/instruction"
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
func (d *Dataset) Distinct(name string, sortOption *SortOption) *Dataset {
	ret := d.LocalSort(name, sortOption).LocalDistinct(name, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(name, 1).LocalDistinct(name, sortOption)
	}
	return ret
}

// Sort sort on specific fields, default to the first field.
// Required Memory: about same size as each partition.
// example usage: Sort(Field(1,2)) means
// sorting on field 1 and 2.
func (d *Dataset) Sort(name string, sortOption *SortOption) *Dataset {
	ret := d.LocalSort(name, sortOption)
	ret = ret.TreeMergeSortedTo(name, 1, 10)
	return ret
}

func (d *Dataset) SortByKey(name string) *Dataset {
	return d.Sort(name, Field(1))
}

// Top streams through total n items, picking reverse ordered k items with O(n*log(k)) complexity.
// Required Memory: about same size as n items in memory
func (d *Dataset) Top(name string, k int, sortOption *SortOption) *Dataset {
	ret := d.LocalTop(name, k, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(name, 1).LocalLimit(name, k, 0)
	}
	return ret
}

func (d *Dataset) LocalDistinct(name string, sortOption *SortOption) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = sortOption.orderByList
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(name, instruction.NewLocalDistinct(sortOption.orderByList))
	return ret
}

func (d *Dataset) LocalSort(name string, sortOption *SortOption) *Dataset {
	if isOrderByEquals(d.IsLocalSorted, sortOption.orderByList) {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = sortOption.orderByList
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.SetInstruction(name, instruction.NewLocalSort(sortOption.orderByList, int(d.GetPartitionSize())*3))
	return ret
}

func (d *Dataset) LocalTop(name string, n int, sortOption *SortOption) *Dataset {
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
