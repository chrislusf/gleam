package flow

import (
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/util"
)

func RunFlowContextSync(fc *FlowContext) {
	var wg sync.WaitGroup
	RunFlowContext(&wg, fc)
	wg.Wait()
}

func RunFlowContext(wg *sync.WaitGroup, fc *FlowContext) {
	wg.Add(1)
	defer wg.Done()

	for _, step := range fc.Steps {
		if step.Output == nil {
			go RunStep(wg, step)
		}
	}
}

func RunDataset(wg *sync.WaitGroup, d *Dataset) {
	d.Lock()
	defer d.Unlock()
	if !d.StartTime.IsZero() {
		return
	}
	d.StartTime = time.Now()
	wg.Add(1)
	defer wg.Done()

	for _, shard := range d.Shards {
		go RunDatasetShard(wg, shard)
	}

	go RunStep(wg, d.Step)
}

func RunDatasetShard(wg *sync.WaitGroup, shard *DatasetShard) {
	wg.Add(1)
	defer wg.Done()
	for bytes := range shard.IncomingChan {
		for _, outgoingChan := range shard.OutgoingChans {
			outgoingChan <- bytes
		}
	}
	for _, outgoingChan := range shard.OutgoingChans {
		close(outgoingChan)
	}
}

func RunStep(wg *sync.WaitGroup, step *Step) {
	wg.Add(1)
	defer wg.Done()

	for _, task := range step.Tasks {
		go RunTask(wg, task)
	}

	for _, ds := range step.Inputs {
		go RunDataset(wg, ds)
	}
}

func RunTask(wg *sync.WaitGroup, task *Task) {
	wg.Add(1)
	defer wg.Done()

	ExecuteTask(wg, task)
}

func ExecuteTask(wg *sync.WaitGroup, task *Task) {
	if task.Step.Function != nil {
		task.Step.Function(task)
	} else if task.Step.NetworkType == OneShardToOneShard {
		cmd := task.Step.Script.GetCommand().ToOsExecCommand()
		// fmt.Printf("cmd: %+v\n", cmd)
		inChan := task.Inputs[0].OutgoingChans[0]
		outChan := task.Outputs[0].IncomingChan
		util.Execute(wg, cmd, inChan, outChan, os.Stderr)
	}
}
