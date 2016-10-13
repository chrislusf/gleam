package flow

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	ret = d.LocalReduce(code)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, []int{1}).LocalReduce(code)
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
	ret = d.LocalSort(indexes).LocalReduceBy(code, indexes)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, indexes).LocalReduceBy(code, indexes)
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
