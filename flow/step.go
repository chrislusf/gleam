package flow

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/instruction"
)

func (fc *FlowContext) NewStep() (step *Step) {
	step = &Step{
		Id:     len(fc.Steps),
		Params: make(map[string]interface{}),
		Meta:   &StepMetadata{},
	}
	fc.Steps = append(fc.Steps, step)
	return
}

func (step *Step) NewTask() (task *Task) {
	task = &Task{Step: step, Id: len(step.Tasks)}
	step.Tasks = append(step.Tasks, task)
	return
}

func (step *Step) SetInstruction(ins instruction.Instruction) {
	step.Name = ins.Name()
	step.Function = ins.Function()
	step.Instruction = ins
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

	task.Stats = &instruction.Stats{}
	if err := task.Step.Function(readers, writers, task.Stats); err != nil {
		log.Printf("Failed to run task %s-%d: %v\n", task.Step.Name, task.Id, err)
	}

	for _, writer := range writers {
		if c, ok := writer.(io.Closer); ok {
			c.Close()
		}
	}
}
