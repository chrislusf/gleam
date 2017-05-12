package flow

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/script"
)

func (fc *FlowContext) NewStep() (step *Step) {
	step = &Step{
		Id:     len(fc.Steps),
		Params: make(map[string]interface{}),
		Meta:   &StepMetadata{IsIdempotent: true},
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

func (step *Step) RunFunction(task *Task) error {
	var readers []io.Reader
	var writers []io.Writer

	for _, reader := range task.InputChans {
		readers = append(readers, reader.Reader)
	}

	for _, shard := range task.OutputShards {
		writers = append(writers, shard.IncomingChan.Writer)
	}

	defer func() {
		for _, writer := range writers {
			if c, ok := writer.(io.Closer); ok {
				c.Close()
			}
		}
	}()

	task.Stat = &pb.InstructionStat{}
	err := task.Step.Function(readers, writers, task.Stat)
	if err != nil {
		log.Printf("Failed to run task %s-%d: %v\n", task.Step.Name, task.Id, err)
	}
	return err
}

func (step *Step) GetScriptCommand() *script.Command {
	if step.Command == nil {
		return step.Script.GetCommand()
	}
	return step.Command
}
