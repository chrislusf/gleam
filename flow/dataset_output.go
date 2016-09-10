package flow

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chrislusf/gleam/util"
)

func (d *Dataset) Output() (out chan []byte) {
	out = make(chan []byte)
	step := d.FlowContext.AddAllToOneStep(d, nil)
	step.Name = "Output"
	step.Function = func(task *Task) {
		var channels []chan []byte
		for _, shard := range task.InputShards {
			channels = append(channels, shard.OutgoingChans...)
		}
		util.MergeChannel(channels, out)
	}
	return
}

func (d *Dataset) SaveTextToFile(fname string) {
	file, err := os.Create(fname)
	if err != nil {
		panic(fmt.Sprintf("Can not create file %s: %v", fname, err))
	}
	defer file.Close()

	d.SaveTextTo(file)
}

func (d *Dataset) SaveTextTo(writer io.Writer) {
	inChan := d.Output()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := util.FprintRowsFromChannel(writer, inChan, "\t", "\n"); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to pipe out: %v\n", err)
		}
	}()

	wg.Add(1)
	RunFlowContext(&wg, d.FlowContext)

	wg.Wait()
}
