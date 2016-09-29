package flow

import (
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/util"
	"github.com/chrislusf/gleam/util/on_interrupt"
)

type FlowRunner interface {
	RunFlowContext(fc *FlowContext)
}

type LocalDriver struct{}

var (
	Local LocalDriver
)

func (r *LocalDriver) RunFlowContext(fc *FlowContext) {
	var wg sync.WaitGroup
	wg.Add(1)
	r.RunFlowContextAsync(&wg, fc)
	wg.Wait()
}

func (r *LocalDriver) RunFlowContextAsync(wg *sync.WaitGroup, fc *FlowContext) {
	defer wg.Done()

	on_interrupt.OnInterrupt(fc.OnInterrupt, nil)

	/*
		for _, ds := range fc.Datasets {
			if ds.IsFinal() {
				wg.Add(1)
				go func(ds *Dataset) {
					RunDataset(wg, ds)
				}(ds)
			}
		}
	*/
	for _, step := range fc.Steps {
		if step.OutputDataset == nil {
			wg.Add(1)
			go func(step *Step) {
				r.RunStep(wg, step)
			}(step)
		}
	}
}

func (r *LocalDriver) RunDataset(wg *sync.WaitGroup, d *Dataset) {
	defer wg.Done()
	d.Lock()
	defer d.Unlock()
	if !d.StartTime.IsZero() {
		return
	}
	d.StartTime = time.Now()

	for _, shard := range d.Shards {
		wg.Add(1)
		go func(shard *DatasetShard) {
			r.RunDatasetShard(wg, shard)
		}(shard)
	}

	wg.Add(1)
	r.RunStep(wg, d.Step)
}

func (r *LocalDriver) RunDatasetShard(wg *sync.WaitGroup, shard *DatasetShard) {
	defer wg.Done()
	shard.ReadyTime = time.Now()
	for bytes := range shard.IncomingChan {
		shard.Counter++
		for _, outgoingChan := range shard.OutgoingChans {
			outgoingChan <- bytes
		}
	}
	for _, outgoingChan := range shard.OutgoingChans {
		close(outgoingChan)
	}
	shard.CloseTime = time.Now()
}

func (r *LocalDriver) RunStep(wg *sync.WaitGroup, step *Step) {
	defer wg.Done()

	for _, task := range step.Tasks {
		wg.Add(1)
		go func(task *Task) {
			r.RunTask(wg, task)
		}(task)
	}

	for _, ds := range step.InputDatasets {
		wg.Add(1)
		go func(ds *Dataset) {
			r.RunDataset(wg, ds)
		}(ds)
	}
}

func (r *LocalDriver) RunTask(wg *sync.WaitGroup, task *Task) {
	defer wg.Done()

	// try to run Function first
	// if failed, try to run shell scripts
	// if failed, try to run lua scripts
	if task.Step.Function != nil {
		task.Step.Function(task)
		return
	}

	// get an exec.Command
	if task.Step.Command == nil {
		task.Step.Command = task.Step.Script.GetCommand()
	}
	cmd := task.Step.Command.ToOsExecCommand()

	if task.Step.NetworkType == OneShardToOneShard {
		// fmt.Printf("cmd: %+v\n", cmd)
		inChan := task.InputShards[0].OutgoingChans[0]
		outChan := task.OutputShards[0].IncomingChan
		wg.Add(1)
		util.Execute(wg, task.Step.Name, cmd, inChan, outChan, task.Step.IsPipe, true, os.Stderr)
	}
}
