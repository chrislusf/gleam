package flow

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/util"
	"github.com/chrislusf/gleam/util/on_interrupt"
)

type FlowRunner interface {
	RunFlowContext(*FlowContext)
}

type FlowOption interface {
	GetFlowRunner() FlowRunner
}

type localDriver struct{}

var (
	local localDriver
)

func (r *localDriver) RunFlowContext(fc *FlowContext) {
	var wg sync.WaitGroup
	wg.Add(1)
	r.RunFlowContextAsync(&wg, fc)
	wg.Wait()
}

func (r *localDriver) RunFlowContextAsync(wg *sync.WaitGroup, fc *FlowContext) {
	defer wg.Done()

	on_interrupt.OnInterrupt(fc.OnInterrupt, nil)

	for _, step := range fc.Steps {
		if step.OutputDataset == nil {
			wg.Add(1)
			go func(step *Step) {
				r.runStep(wg, step)
			}(step)
		}
	}
}

func (r *localDriver) runDataset(wg *sync.WaitGroup, d *Dataset) {
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
			r.runDatasetShard(wg, shard)
		}(shard)
	}

	wg.Add(1)
	r.runStep(wg, d.Step)
}

func (r *localDriver) runDatasetShard(wg *sync.WaitGroup, shard *DatasetShard) {
	defer wg.Done()
	shard.ReadyTime = time.Now()

	var writers []io.Writer
	for _, outgoingChan := range shard.OutgoingChans {
		writers = append(writers, outgoingChan.Writer)
	}

	util.BufWrites(writers, func(writers []io.Writer) {
		w := io.MultiWriter(writers...)
		n, _ := io.Copy(w, shard.IncomingChan.Reader)
		// println("shard", shard.Name(), "moved", n, "bytes.")
		shard.Counter = n
		shard.CloseTime = time.Now()
	})

	for _, outgoingChan := range shard.OutgoingChans {
		outgoingChan.Writer.Close()
	}
}

func (r *localDriver) runStep(wg *sync.WaitGroup, step *Step) {
	defer wg.Done()

	for _, task := range step.Tasks {
		wg.Add(1)
		go func(task *Task) {
			r.runTask(wg, task)
		}(task)
	}

	for _, ds := range step.InputDatasets {
		wg.Add(1)
		go func(ds *Dataset) {
			r.runDataset(wg, ds)
		}(ds)
	}
}

func (r *localDriver) runTask(wg *sync.WaitGroup, task *Task) {
	defer wg.Done()

	// try to run Function first
	// if failed, try to run shell scripts
	// if failed, try to run lua scripts
	if task.Step.Function != nil {
		// each function should close its own Piper output writer
		// and close it's own Piper input reader
		task.Step.RunFunction(task)
		return
	}

	// get an exec.Command
	scriptCommand := task.Step.GetScriptCommand()
	execCommand := scriptCommand.ToOsExecCommand()

	if task.Step.NetworkType == OneShardToOneShard {
		// fmt.Printf("execCommand: %+v\n", execCommand)
		reader := task.InputChans[0].Reader
		writer := task.OutputShards[0].IncomingChan.Writer
		wg.Add(1)
		prevIsPipe := task.InputShards[0].Dataset.Step.IsPipe
		util.Execute(context.Background(), wg, task.Step.Name, execCommand, reader, writer, prevIsPipe, task.Step.IsPipe, true, os.Stderr)
	} else {
		println("network type:", task.Step.NetworkType)
	}
}
