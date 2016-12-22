// Package flow contains data structure for computation.
// Mostly Dataset operations such as Map/Reduce/Join/Sort etc.
package flow

import (
	"math/rand"
	"time"

	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
)

func New() (fc *FlowContext) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	fc = &FlowContext{
		PrevScriptType: "luajit",
		Scripts: map[string]func() script.Script{
			"luajit": script.NewLuajitScript,
			"lua":    script.NewLuaScript,
		},
		HashCode: r.Uint32(),
	}
	return
}

func (fc *FlowContext) Run(options ...FlowOption) {
	if len(options) == 0 {
		Local.RunFlowContext(fc)
	} else {
		for _, option := range options {
			option.GetFlowRunner().RunFlowContext(fc)
		}
	}
}

func (fc *FlowContext) newNextDataset(shardSize int) (ret *Dataset) {
	ret = newDataset(fc)
	ret.setupShard(shardSize)
	return
}

// the tasks should run on the source dataset shard
func (f *FlowContext) AddOneToOneStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = OneShardToOneShard
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	if input == nil {
		task := step.NewTask()
		if output != nil && output.Shards != nil {
			FromTaskToDatasetShard(task, output.GetShards()[0])
		}
		return
	}

	// setup the network
	for i, shard := range input.GetShards() {
		task := step.NewTask()
		if output != nil && output.Shards != nil {
			FromTaskToDatasetShard(task, output.GetShards()[i])
		}
		FromDatasetShardToTask(shard, task)
	}
	return
}

// the task should run on the destination dataset shard
func (f *FlowContext) AddAllToOneStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = AllShardToOneShard
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	task := step.NewTask()
	if output != nil {
		FromTaskToDatasetShard(task, output.GetShards()[0])
	}
	for _, shard := range input.GetShards() {
		FromDatasetShardToTask(shard, task)
	}
	return
}

// the task should run on the source dataset shard
// input is nil for initial source dataset
func (f *FlowContext) AddOneToAllStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = OneShardToAllShard
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	task := step.NewTask()
	if input != nil {
		FromDatasetShardToTask(input.GetShards()[0], task)
	}
	for _, shard := range output.GetShards() {
		FromTaskToDatasetShard(task, shard)
	}
	return
}

func (f *FlowContext) AddOneToEveryNStep(input *Dataset, n int, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = OneShardToEveryNShard
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	m := len(input.GetShards())
	for i, inShard := range input.GetShards() {
		task := step.NewTask()
		for k := 0; k < n; k++ {
			FromTaskToDatasetShard(task, output.GetShards()[k*m+i])
		}
		FromDatasetShardToTask(inShard, task)
	}
	return
}

func (f *FlowContext) AddLinkedNToOneStep(input *Dataset, m int, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = LinkedNShardToOneShard
	FromStepToDataset(step, output)
	FromDatasetToStep(input, step)

	// setup the network
	for i, outShard := range output.GetShards() {
		task := step.NewTask()
		FromTaskToDatasetShard(task, outShard)
		for k := 0; k < m; k++ {
			FromDatasetShardToTask(input.GetShards()[i*m+k], task)
		}
	}
	return
}

// All dataset should have the same number of shards.
func (f *FlowContext) MergeDatasets1ShardTo1Step(inputs []*Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = MergeTwoShardToOneShard
	FromStepToDataset(step, output)
	for _, input := range inputs {
		FromDatasetToStep(input, step)
	}

	// setup the network
	if output != nil {
		for shardId, outShard := range output.Shards {
			task := step.NewTask()
			for _, input := range inputs {
				FromDatasetShardToTask(input.GetShards()[shardId], task)
			}
			FromTaskToDatasetShard(task, outShard)
		}
	}
	return
}

func FromStepToDataset(step *Step, output *Dataset) {
	if output == nil {
		return
	}
	output.Step = step
	step.OutputDataset = output
}

func FromDatasetToStep(input *Dataset, step *Step) {
	if input == nil {
		return
	}
	step.InputDatasets = append(step.InputDatasets, input)
	input.ReadingSteps = append(input.ReadingSteps, step)
}

func FromDatasetShardToTask(shard *DatasetShard, task *Task) {
	piper := util.NewPiper()
	shard.ReadingTasks = append(shard.ReadingTasks, task)
	shard.OutgoingChans = append(shard.OutgoingChans, piper)
	task.InputShards = append(task.InputShards, shard)
	task.InputChans = append(task.InputChans, piper)
}

func FromTaskToDatasetShard(task *Task, shard *DatasetShard) {
	if shard != nil {
		task.OutputShards = append(task.OutputShards, shard)
	}
}
