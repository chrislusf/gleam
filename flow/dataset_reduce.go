package flow

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	return d.LocalReduce(code).MergeSortedTo(1).LocalReduce(code)
}

func (d *Dataset) ReduceByKey(code string) (ret *Dataset) {
	// TODO avoid local reduce twice if partition is one
	return d.LocalSort().LocalReduceByKey(code).MergeSortedTo(1).LocalReduceByKey(code)
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
