package flow

func (d *Dataset) Map(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Map"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Map(code)
	return ret
}

func (d *Dataset) ForEach(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "ForEach"
	step.Script = d.FlowContext.CreateScript()
	step.Script.ForEach(code)
	return ret
}

func (d *Dataset) FlatMap(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "FlatMap"
	step.Script = d.FlowContext.CreateScript()
	step.Script.FlatMap(code)
	return ret
}

func (d *Dataset) Filter(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Filter"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Filter(code)
	return ret
}

func add1ShardTo1Step(d *Dataset) (ret *Dataset, step *Step) {
	ret = d.FlowContext.newNextDataset(len(d.Shards))
	step = d.FlowContext.AddOneToOneStep(d, ret)
	return
}

func (d *Dataset) Select(indexes ...int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Map"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Select(indexes)
	return ret
}
