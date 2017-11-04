package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// Union union multiple Datasets as one Dataset
func (this *Dataset) Union(name string, others []*Dataset, isParallel bool) *Dataset {
	ret := this.Flow.NewNextDataset(len(this.Shards))
	inputs := []*Dataset{this}
	inputs = append(inputs, others...)
	step := this.Flow.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(name, instruction.NewUnion(isParallel))
	return ret
}
