package flow

import (
	"io"
)

func (fc *FlowContext) NewStep() (step *Step) {
	step = &Step{
		Id:     len(fc.Steps),
		Params: make(map[string]interface{}),
	}
	fc.Steps = append(fc.Steps, step)
	return
}

func (step *Step) NewTask() (task *Task) {
	task = &Task{Step: step, Id: len(step.Tasks)}
	step.Tasks = append(step.Tasks, task)
	return
}

func (step *Step) RunFunction(task *Task) {
	var readers []io.Reader
	var writers []io.Writer

	for _, reader := range task.InputChans {
		readers = append(readers, reader.Reader)
	}

	for _, shard := range task.OutputShards {
		writers = append(writers, shard.IncomingChan.Writer)
	}

	task.Step.Function(readers, writers, task)

	for _, writer := range writers {
		if c, ok := writer.(io.Closer); ok {
			c.Close()
		}
	}

}
