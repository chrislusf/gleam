package flow

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chrislusf/gleam/util"
)

func (d *Dataset) Output(f func(io.Reader) error) {
	step := d.FlowContext.AddAllToOneStep(d, nil)
	step.IsOnDriverSide = true
	step.Name = "Output"
	step.Function = func(task *Task) {
		var wg sync.WaitGroup
		for _, shard := range task.InputShards {
			for _, outChan := range shard.OutgoingChans {
				wg.Add(1)
				go func(outChan *util.Piper) {
					defer wg.Done()
					f(outChan.Reader)
					outChan.Reader.Close()
				}(outChan)
			}
		}
		wg.Wait()
	}
}

func (d *Dataset) PipeOut(writer io.Writer) {
	fn := func(inChan io.Reader) error {
		if d.Step.IsPipe {
			_, err := io.Copy(writer, inChan)
			return err
		}
		return util.FprintRowsFromChannel(inChan, writer, "\t", "\n")
	}
	d.Output(fn)

	d.FlowContext.Runner.RunFlowContext(d.FlowContext)
}

func (d *Dataset) Fprintf(writer io.Writer, format string) {
	fn := func(inChan io.Reader) error {
		if d.Step.IsPipe {
			return util.TsvPrintf(inChan, writer, format)
		}
		return util.Fprintf(inChan, writer, format)
	}
	d.Output(fn)

	d.FlowContext.Runner.RunFlowContext(d.FlowContext)
}

func (d *Dataset) SaveOneRowTo(decodedObjects ...interface{}) {
	fn := func(inChan io.Reader) error {
		if d.Step.IsPipe {
			return util.TakeTsv(inChan, 1, func(args []string) error {
				for i, o := range decodedObjects {
					if i >= len(args) {
						break
					}
					if v, ok := o.(*string); ok {
						*v = args[i]
					} else {
						return fmt.Errorf("Should save to *string.")
					}
				}
				return nil
			})
		}

		return util.TakeMessage(inChan, 1, func(encodedBytes []byte) error {
			if err := util.DecodeRowTo(encodedBytes, decodedObjects...); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				return err
			}
			return nil
		})
	}
	d.Output(fn)

	d.FlowContext.Runner.RunFlowContext(d.FlowContext)
}
