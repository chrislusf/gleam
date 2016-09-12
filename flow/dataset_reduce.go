package flow

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	return d.LocalSort().LocalReduce(code).MergeSortedTo(1).LocalReduce(code)
}

func (d *Dataset) LocalReduce(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalReduce"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.CreateScript()
	step.Script.Reduce(code)
	return ret
}
