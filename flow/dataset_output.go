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

func (d *Dataset) SaveTextTo(writer io.Writer, format string) {
	inChan := d.Output()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for encodedBytes := range inChan {
			var decodedObjects []interface{}
			var err error
			// fmt.Printf("chan input encoded: %s\n", string(encodedBytes))
			if decodedObjects, err = util.DecodeRow(encodedBytes); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				continue
			}

			fmt.Fprintf(writer, format, decodedObjects...)

			writer.Write([]byte("\n"))
		}
	}()

	wg.Add(1)
	RunFlowContext(&wg, d.FlowContext)

	wg.Wait()
}

func (d *Dataset) SaveFinalRowTo(decodedObjects ...interface{}) {
	inChan := d.Output()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for encodedBytes := range inChan {
			if err := util.DecodeRowTo(encodedBytes, decodedObjects...); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				continue
			}
		}
	}()

	wg.Add(1)
	RunFlowContext(&wg, d.FlowContext)

	wg.Wait()
}
