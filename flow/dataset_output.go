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
		for _, shard := range task.Inputs {
			channels = append(channels, shard.OutgoingChans...)
		}
		util.MergeChannel(channels, out)
	}
	return
}

func (d *Dataset) SaveBytesToFile(fname string) {
	outChan := d.Output()

	file, err := os.Create(fname)
	if err != nil {
		panic(fmt.Sprintf("Can not create file %s: %v", fname, err))
	}
	defer file.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range outChan {
			file.Write(b)
		}
	}()

	RunFlowContext(&wg, d.FlowContext)

	wg.Wait()

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
	outChan := d.Output()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for line := range outChan {
			writer.Write(line)
			writer.Write([]byte("\n"))
		}
	}()

	RunFlowContext(&wg, d.FlowContext)

	wg.Wait()
}
