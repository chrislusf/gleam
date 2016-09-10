package flow

func (d *Dataset) Reduce(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Reduce"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.CreateScript()
	step.Script.Reduce(code)
	return ret
}
