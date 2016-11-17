package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

type SortOption struct {
	orderByList    []instruction.OrderBy
	MemorySizeInMB int
}

// By groups the indexes, usually start from 1, into a []int
func Field(indexes ...int) *SortOption {
	ret := &SortOption{}
	for _, index := range indexes {
		ret.orderByList = append(ret.orderByList, instruction.OrderBy{
			Index: index,
			Order: instruction.Ascending,
		})
	}
	return ret
}

func OrderBy(index int, ascending bool) *SortOption {
	ret := &SortOption{
		orderByList: []instruction.OrderBy{
			instruction.OrderBy{
				Index: index,
				Order: instruction.Descending,
			},
		},
	}
	if ascending {
		ret.orderByList[0].Order = instruction.Ascending
	}
	return ret
}

// OrderBy chains a list of sorting order by
func (o *SortOption) By(index int, ascending bool) *SortOption {
	return o
}

func (o *SortOption) Memory(sizeInMB int) *SortOption {
	o.MemorySizeInMB = sizeInMB
	return o
}

// return a list of indexes
func (o *SortOption) Indexes() []int {
	var ret []int
	for _, x := range o.orderByList {
		ret = append(ret, x.Index)
	}
	return ret
}

func concat(sortOptions []*SortOption) *SortOption {
	if len(sortOptions) == 0 {
		return Field(1)
	}
	ret := &SortOption{}
	for _, sortOption := range sortOptions {
		ret.orderByList = append(ret.orderByList, sortOption.orderByList...)
		ret.MemorySizeInMB += sortOption.MemorySizeInMB
	}
	return ret
}
