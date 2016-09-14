package flow

func (d *Dataset) GroupByKey() *Dataset {
	return d.LocalSort().LocalGroupByKey()
}

func (d *Dataset) LocalGroupByKey() *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalGroupByKey"
	step.Script = d.FlowContext.CreateScript()
	step.Script.GroupByKey()
	return ret
}
