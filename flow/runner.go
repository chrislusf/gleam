package flow

import (
	"io"
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
	var writers []io.Writer
	for _, outgoingChan := range shard.OutgoingChans {
		writers = append(writers, outgoingChan.Writer)
	}
	w := io.MultiWriter(writers...)
	n, _ := io.Copy(w, shard.IncomingChan.Reader)
	for _, outgoingChan := range shard.OutgoingChans {
		outgoingChan.Writer.Close()
	}
	// println("shard", shard.Name(), "moved", n, "bytes.")
	shard.Counter = n
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
		// each function should close its own Piper output writer
		// and close it's own Piper input reader
		task.Step.RunFunction(task)
		return
	}

	// get an exec.Command
	if task.Step.Command == nil {
		task.Step.Command = task.Step.Script.GetCommand()
	}
	execCommand := task.Step.Command.ToOsExecCommand()

	if task.Step.NetworkType == OneShardToOneShard {
		// fmt.Printf("execCommand: %+v\n", execCommand)
		reader := task.InputChans[0].Reader
		writer := task.OutputShards[0].IncomingChan.Writer
		wg.Add(1)
		prevIsPipe := task.InputShards[0].Dataset.Step.IsPipe
		util.Execute(wg, task.Step.Name, execCommand, reader, writer, prevIsPipe, task.Step.IsPipe, true, os.Stderr)
	} else {
		println("network type:", task.Step.NetworkType)
	}
}
