package flow

import (
	"github.com/chrislusf/gleam/script"
)

func (d *Dataset) Map(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Map"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.CreateScript()
	step.Script.Map(code)
	return ret
}

func (d *Dataset) ForEach(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "ForEach"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.CreateScript()
	step.Script.ForEach(code)
	return ret
}

func (d *Dataset) FlatMap(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "FlatMap"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.CreateScript()
	step.Script.FlatMap(code)
	return ret
}

func (d *Dataset) Filter(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Filter"
	step.NetworkType = OneShardToOneShard
	step.Script = d.FlowContext.CreateScript()
	step.Script.Filter(code)
	return ret
}

func (d *Dataset) Pipe(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Pipe"
	step.IsPipe = true
	step.NetworkType = OneShardToOneShard
	step.Command = script.NewShellScript().Pipe(code).GetCommand()
	return ret
}

func add1ShardTo1Step(d *Dataset) (ret *Dataset, step *Step) {
	ret = d.FlowContext.newNextDataset(len(d.Shards))
	step = d.FlowContext.AddOneToOneStep(d, ret)
	return
}
