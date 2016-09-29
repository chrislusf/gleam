package flow

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chrislusf/gleam/util"
)

func (d *Dataset) Output(f func(chan []byte)) {
	step := d.FlowContext.AddAllToOneStep(d, nil)
	step.IsOnDriverSide = true
	step.Name = "Output"
	step.Function = func(task *Task) {
		var wg sync.WaitGroup
		for _, shard := range task.InputShards {
			for _, c := range shard.OutgoingChans {
				wg.Add(1)
				go func(c chan []byte) {
					defer wg.Done()
					f(c)
				}(c)
			}
		}
		wg.Wait()
	}
}

func (d *Dataset) Fprintf(writer io.Writer, format string) {
	fn := func(inChan chan []byte) {
		util.Fprintf(inChan, writer, format)
	}
	d.Output(fn)

	d.FlowContext.Runner.RunFlowContext(d.FlowContext)
}

func (d *Dataset) SaveOneRowTo(decodedObjects ...interface{}) {
	fn := func(inChan chan []byte) {
		for encodedBytes := range inChan {
			if err := util.DecodeRowTo(encodedBytes, decodedObjects...); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				continue
			}
		}
	}
	d.Output(fn)

	d.FlowContext.Runner.RunFlowContext(d.FlowContext)
}
