package plan

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
)

func TranslateToInstructionSet(taskGroups *TaskGroup) (ret *pb.InstructionSet) {
	ret = &pb.InstructionSet{}
	lastShards := taskGroups.Tasks[len(taskGroups.Tasks)-1].OutputShards
	if len(lastShards) > 0 {
		ret.ReaderCount = int32(len(lastShards[0].ReadingTasks))
	}
	for _, task := range taskGroups.Tasks {
		instruction := translateToInstruction(task)
		if instruction != nil {
			ret.Instructions = append(ret.Instructions, instruction)
		}
	}
	return
}

func translateToInstruction(task *flow.Task) (ret *pb.Instruction) {

	if task.Step.IsOnDriverSide {
		return nil
	}

	// try to run Instruction first
	// if failed, try to run shell scripts
	if task.Step.Instruction != nil {
		ret = task.Step.Instruction.SerializeToCommand()
	} else {
		// Command can come from Pipe() directly
		// get an exec.Command
		// println("processing step:", task.Step.Name)
		command := task.Step.GetScriptCommand()

		ret = &pb.Instruction{
			Script: &pb.Instruction_Script{
				IsPipe: task.Step.IsPipe,
				Path:   command.Path,
				Args:   command.Args,
				Env:    command.Env,
			},
		}
	}

	ret.StepId = int32(task.Step.Id)
	ret.TaskId = int32(task.Id)

	return
}
