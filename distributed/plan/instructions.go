package plan

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/msg"
	"github.com/golang/protobuf/proto"
)

func TranslateToInstructionSet(taskGroups *TaskGroup) (ret *msg.InstructionSet) {
	ret = &msg.InstructionSet{}
	lastShards := taskGroups.Tasks[len(taskGroups.Tasks)-1].OutputShards
	if len(lastShards) > 0 {
		ret.ReaderCount = proto.Int32(int32(len(lastShards[0].ReadingTasks)))
	}
	for _, task := range taskGroups.Tasks {
		instruction := translateToInstruction(task)
		if instruction != nil {
			ret.Instructions = append(ret.Instructions, instruction)
		}
	}
	return
}

func translateToInstruction(task *flow.Task) (ret *msg.Instruction) {

	if task.Step.IsOnDriverSide {
		return nil
	}

	// try to run Instruction first
	// if failed, try to run shell scripts
	// if failed, try to run lua scripts

	if task.Step.Instruction != nil {
		return task.Step.Instruction.SerializeToCommand()
	}

	// Command can come from Pipe() directly
	// get an exec.Command
	// println("processing step:", task.Step.Name)
	command := task.Step.GetScriptCommand()

	return &msg.Instruction{
		Name: proto.String(task.Step.Name),
		Script: &msg.Script{
			IsPipe: proto.Bool(task.Step.IsPipe),
			Path:   proto.String(command.Path),
			Args:   command.Args,
			Env:    command.Env,
		},
	}
}
