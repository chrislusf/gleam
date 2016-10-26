package plan

import (
	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/flow"
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

	if task.Step.FunctionType == flow.TypeLocalSort {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			LocalSort: &cmd.LocalSort{
				OrderBys: getOrderBys(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypePipeAsArgs {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			PipeAsArgs: &cmd.PipeAsArgs{
				Code: proto.String(task.Step.Params["code"].(string)),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeMergeSortedTo {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			MergeSortedTo: &cmd.MergeSortedTo{
				OrderBys: getOrderBys(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeJoinPartitionedSorted {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			JoinPartitionedSorted: &cmd.JoinPartitionedSorted{
				IsLeftOuterJoin:  proto.Bool(task.Step.Params["isLeftOuterJoin"].(bool)),
				IsRightOuterJoin: proto.Bool(task.Step.Params["isRightOuterJoin"].(bool)),
				Indexes:          getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeCoGroupPartitionedSorted {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			CoGroupPartitionedSorted: &cmd.CoGroupPartitionedSorted{
				Indexes: getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeCollectPartitions {
		return &cmd.Instruction{
			Name:              proto.String(task.Step.Name),
			CollectPartitions: &cmd.CollectPartitions{},
		}
	}

	if task.Step.FunctionType == flow.TypeScatterPartitions {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			ScatterPartitions: &cmd.ScatterPartitions{
				ShardCount: proto.Int32(int32(task.Step.Params["shardCount"].(int))),
				Indexes:    getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeRoundRobin {
		return &cmd.Instruction{
			Name:       proto.String(task.Step.Name),
			RoundRobin: &cmd.RoundRobin{},
		}
	}

	if task.Step.FunctionType == flow.TypeInputSplitReader {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			InputSplitReader: &cmd.InputSplitReader{
				InputType: proto.String(task.Step.Params["inputType"].(string)),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeLocalTop {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			LocalTop: &cmd.LocalTop{
				N:        proto.Int32(int32(task.Step.Params["n"].(int))),
				OrderBys: getOrderBys(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeBroadcast {
		return &cmd.Instruction{
			Name:      proto.String(task.Step.Name),
			Broadcast: &cmd.Broadcast{},
		}
	}

	if task.Step.FunctionType == flow.TypeLocalHashAndJoinWith {
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
	storedValues := task.Step.Params["orderBys"].([]flow.OrderBy)
	for _, o := range storedValues {
		orderBys = append(orderBys, &cmd.OrderBy{
			Index: proto.Int32(int32(o.Index)),
			Order: proto.Int32(int32(o.Order)),
		})
	}
	return
}
