package flow

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	ret = d.LocalReduce(code)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1).LocalReduce(code)
	}
	return ret
}

func (d *Dataset) ReduceByKey(code string) (ret *Dataset) {
	ret = d.LocalSort().LocalReduceByKey(code)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1).LocalReduceByKey(code)
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

func (d *Dataset) LocalReduceByKey(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalReduceByKey"
	step.Script = d.FlowContext.CreateScript()
	step.Script.ReduceByKey(code)
	return ret
}
