package plan

import (
	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/flow"
	ins "github.com/chrislusf/gleam/instruction"
	"github.com/golang/protobuf/proto"
)

func TranslateToInstructionSet(taskGroups *TaskGroup) (ret *cmd.InstructionSet) {
	ret = &cmd.InstructionSet{}
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

func translateToInstruction(task *flow.Task) (ret *cmd.Instruction) {

	if task.Step.IsOnDriverSide {
		return nil
	}

	// try to run Function first
	// if failed, try to run shell scripts
	// if failed, try to run lua scripts

	if task.Step.FunctionType == ins.TypeLocalSort {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypePipeAsArgs {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeMergeSortedTo {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeJoinPartitionedSorted {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			JoinPartitionedSorted: &cmd.JoinPartitionedSorted{
				IsLeftOuterJoin:  proto.Bool(task.Step.Params["isLeftOuterJoin"].(bool)),
				IsRightOuterJoin: proto.Bool(task.Step.Params["isRightOuterJoin"].(bool)),
				Indexes:          getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == ins.TypeCoGroupPartitionedSorted {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			CoGroupPartitionedSorted: &cmd.CoGroupPartitionedSorted{
				Indexes: getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == ins.TypeCollectPartitions {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeScatterPartitions {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeRoundRobin {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeInputSplitReader {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeLocalTop {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeBroadcast {
		return task.Step.Instruction.SerializeToCommand()
	}

	if task.Step.FunctionType == ins.TypeLocalHashAndJoinWith {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			LocalHashAndJoinWith: &cmd.LocalHashAndJoinWith{
				Indexes: getIndexes(task),
			},
		}
	}

	// Command can come from Pipe() directly
	// get an exec.Command
	// println("processing step:", task.Step.Name)
	if task.Step.Command == nil {
		task.Step.Command = task.Step.Script.GetCommand()
	}
	command := task.Step.Command

	return &cmd.Instruction{
		Name: proto.String(task.Step.Name),
		Script: &cmd.Script{
			IsPipe: proto.Bool(task.Step.IsPipe),
			Path:   proto.String(command.Path),
			Args:   command.Args,
			Env:    command.Env,
		},
	}
}

func getIndexes(task *flow.Task) (indexes []int32) {
	storedValues := task.Step.Params["indexes"].([]int)
	for _, x := range storedValues {
		indexes = append(indexes, int32(x))
	}
	return
}

func getOrderBys(task *flow.Task) (orderBys []*cmd.OrderBy) {
	storedValues := task.Step.Params["orderBys"].([]ins.OrderBy)
	for _, o := range storedValues {
		orderBys = append(orderBys, &cmd.OrderBy{
			Index: proto.Int32(int32(o.Index)),
			Order: proto.Int32(int32(o.Order)),
		})
	}
	return
}
