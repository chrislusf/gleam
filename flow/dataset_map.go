package flow

func (d *Dataset) Map(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Map"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.GetScript()
	step.Script.Map(code)
	return ret
}

func add1ShardTo1Step(d *Dataset) (ret *Dataset, step *Step) {
	ret = d.FlowContext.newNextDataset(len(d.Shards))
	step = d.FlowContext.AddOneToOneStep(d, ret)
	return
}
