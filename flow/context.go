// Package flow contains data structure for computation.
// Mostly Dataset operations such as Map/Reduce/Join/Sort etc.
package flow

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/util"
)

func New(name string) (fc *Flow) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	fc = &Flow{
		Name:     name,
		HashCode: r.Uint32(),
	}
	return
}

func (fc *Flow) Run(options ...FlowOption) {
	fc.RunContext(context.Background(), options...)
}

func (fc *Flow) RunContext(ctx context.Context, options ...FlowOption) {

	if !gio.HasInitalized {
		println("gio.Init() is required right after main() if pure go mapper or reducer is used.")
		os.Exit(1)
	}

	if len(options) == 0 {
		Local.RunFlowContext(ctx, fc)
	} else {
		for _, option := range options {
			option.GetFlowRunner().RunFlowContext(ctx, fc)
		}
	}
}

func (fc *Flow) NewNextDataset(shardSize int) (ret *Dataset) {
	ret = newDataset(fc)
	setupDatasetShard(ret, shardSize)
	return
}

// the tasks should run on the source dataset shard
func (f *Flow) AddOneToOneStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = OneShardToOneShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	if input == nil {
		task := step.NewTask()
		if output != nil && output.Shards != nil {
			fromTaskToDatasetShard(task, output.GetShards()[0])
		}
		return
	}

	// setup the network
	for i, shard := range input.GetShards() {
		task := step.NewTask()
		if output != nil && output.Shards != nil {
			fromTaskToDatasetShard(task, output.GetShards()[i])
		}
		fromDatasetShardToTask(shard, task)
	}
	return
}

// the task should run on the destination dataset shard
func (f *Flow) AddAllToOneStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = AllShardToOneShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network
	task := step.NewTask()
	if output != nil {
		fromTaskToDatasetShard(task, output.GetShards()[0])
	}
	for _, shard := range input.GetShards() {
		fromDatasetShardToTask(shard, task)
	}
	return
}

// the task should run on the source dataset shard
// input is nil for initial source dataset
func (f *Flow) AddOneToAllStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = OneShardToAllShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network
	task := step.NewTask()
	if input != nil {
		fromDatasetShardToTask(input.GetShards()[0], task)
	}
	for _, shard := range output.GetShards() {
		fromTaskToDatasetShard(task, shard)
	}
	return
}

func (f *Flow) AddAllToAllStep(input *Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = AllShardTOAllShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network
	task := step.NewTask()
	for _, shard := range input.GetShards() {
		fromDatasetShardToTask(shard, task)
	}
	for _, shard := range output.GetShards() {
		fromTaskToDatasetShard(task, shard)
	}
	return
}

func (f *Flow) AddOneToEveryNStep(input *Dataset, n int, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = OneShardToEveryNShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network
	m := len(input.GetShards())
	for i, inShard := range input.GetShards() {
		task := step.NewTask()
		for k := 0; k < n; k++ {
			fromTaskToDatasetShard(task, output.GetShards()[k*m+i])
		}
		fromDatasetShardToTask(inShard, task)
	}
	return
}

func (f *Flow) AddLinkedNToOneStep(input *Dataset, m int, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = LinkedNShardToOneShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network
	for i, outShard := range output.GetShards() {
		task := step.NewTask()
		fromTaskToDatasetShard(task, outShard)
		for k := 0; k < m; k++ {
			fromDatasetShardToTask(input.GetShards()[i*m+k], task)
		}
	}
	return
}

// All dataset should have the same number of shards.
func (f *Flow) MergeDatasets1ShardTo1Step(inputs []*Dataset, output *Dataset) (step *Step) {
	step = f.NewStep()
	step.NetworkType = MergeTwoShardToOneShard
	fromStepToDataset(step, output)
	for _, input := range inputs {
		fromDatasetToStep(input, step)
	}

	// setup the network
	if output != nil {
		for shardId, outShard := range output.Shards {
			task := step.NewTask()
			for _, input := range inputs {
				fromDatasetShardToTask(input.GetShards()[shardId], task)
			}
			fromTaskToDatasetShard(task, outShard)
		}
	}
	return
}

func fromStepToDataset(step *Step, output *Dataset) {
	if output == nil {
		return
	}
	output.Step = step
	step.OutputDataset = output
}

func fromDatasetToStep(input *Dataset, step *Step) {
	if input == nil {
		return
	}
	step.InputDatasets = append(step.InputDatasets, input)
	input.ReadingSteps = append(input.ReadingSteps, step)
}

func setupDatasetShard(d *Dataset, n int) {
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Id:           i,
			Dataset:      d,
			IncomingChan: util.NewPiper(),
		}
		d.Shards = append(d.Shards, ds)
	}
}

func fromDatasetShardToTask(shard *DatasetShard, task *Task) {
	piper := util.NewPiper()
	shard.ReadingTasks = append(shard.ReadingTasks, task)
	shard.OutgoingChans = append(shard.OutgoingChans, piper)
	task.InputShards = append(task.InputShards, shard)
	task.InputChans = append(task.InputChans, piper)
}

func fromTaskToDatasetShard(task *Task, shard *DatasetShard) {
	if shard != nil {
		task.OutputShards = append(task.OutputShards, shard)
	}
}
