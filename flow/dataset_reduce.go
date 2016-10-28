package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	ret = d.LocalReduce(code)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, []instruction.OrderBy{instruction.OrderBy{1, instruction.Ascending}}).LocalReduce(code)
	}
	return ret
}

func (d *Dataset) LocalReduce(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalReduce"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Reduce(code)
	return ret
}

func (d *Dataset) ReduceBy(code string, indexes ...int) (ret *Dataset) {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	ret = d.LocalSort(orderBys).LocalReduceBy(code, indexes)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys).LocalReduceBy(code, indexes)
	}
	return ret
}

func (d *Dataset) LocalReduceBy(code string, indexes []int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalReduceBy"
	step.Script = d.FlowContext.CreateScript()
	step.Script.ReduceBy(code, indexes)
	return ret
}
