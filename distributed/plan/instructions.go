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
				InputShardLocation:  flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[0]),
				OutputShardLocation: flowDatasetShardsToCmdDatasetShardLocation(task.OutputShards[0]),
				Indexes:             getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypePipeAsArgs {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			PipeAsArgs: &cmd.PipeAsArgs{
				InputShardLocation:  flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[0]),
				OutputShardLocation: flowDatasetShardsToCmdDatasetShardLocation(task.OutputShards[0]),
				Code:                proto.String(task.Step.Params["code"].(string)),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeMergeSortedTo {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			MergeSortedTo: &cmd.MergeSortedTo{
				InputShardLocations: flowDatasetShardsToCmdDatasetShardLocations(task.InputShards),
				OutputShardLocation: flowDatasetShardsToCmdDatasetShardLocation(task.OutputShards[0]),
				Indexes:             getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeJoinPartitionedSorted {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			JoinPartitionedSorted: &cmd.JoinPartitionedSorted{
				LeftInputShardLocation:  flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[0]),
				RightInputShardLocation: flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[1]),
				OutputShardLocation:     flowDatasetShardsToCmdDatasetShardLocation(task.OutputShards[0]),
				IsLeftOuterJoin:         proto.Bool(false),
				IsRightOuterJoin:        proto.Bool(false),
				Indexes:                 getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeCoGroupPartitionedSorted {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			CoGroupPartitionedSorted: &cmd.CoGroupPartitionedSorted{
				LeftInputShardLocation:  flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[0]),
				RightInputShardLocation: flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[1]),
				OutputShardLocation:     flowDatasetShardsToCmdDatasetShardLocation(task.OutputShards[0]),
				Indexes:                 getIndexes(task),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeCollectPartitions {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			CollectPartitions: &cmd.CollectPartitions{
				InputShardLocations: flowDatasetShardsToCmdDatasetShardLocations(task.InputShards),
				OutputShardLocation: flowDatasetShardsToCmdDatasetShardLocation(task.OutputShards[0]),
			},
		}
	}

	if task.Step.FunctionType == flow.TypeScatterPartitions {
		return &cmd.Instruction{
			Name: proto.String(task.Step.Name),
			ScatterPartitions: &cmd.ScatterPartitions{
				InputShardLocation:   flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[0]),
				OutputShardLocations: flowDatasetShardsToCmdDatasetShardLocations(task.OutputShards),
				ShardCount:           proto.Int32(int32(task.Step.Params["shardCount"].(int))),
				Indexes:              getIndexes(task),
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
			InputShardLocation:  flowDatasetShardsToCmdDatasetShardLocation(task.InputShards[0]),
			OutputShardLocation: flowDatasetShardsToCmdDatasetShardLocation(task.OutputShards[0]),
			IsPipe:              proto.Bool(task.Step.IsPipe),
			Path:                proto.String(command.Path),
			Args:                command.Args,
			Env:                 command.Env,
		},
	}
}

func flowDatasetShardToCmdDatasetShard(shard *flow.DatasetShard) *cmd.DatasetShard {
	return &cmd.DatasetShard{
		FlowName:       proto.String(""),
		DatasetId:      proto.Int32(int32(shard.Dataset.Id)),
		DatasetShardId: proto.Int32(int32(shard.Id)),
		FlowHashCode:   proto.Uint32(shard.Dataset.FlowContext.HashCode),
	}
}

func flowDatasetShardsToCmdDatasetShardLocations(shards []*flow.DatasetShard) (ret []*cmd.DatasetShardLocation) {
	for _, shard := range shards {
		ret = append(ret, flowDatasetShardsToCmdDatasetShardLocation(shard))
	}
	return
}

func flowDatasetShardsToCmdDatasetShardLocation(shard *flow.DatasetShard) *cmd.DatasetShardLocation {
	return &cmd.DatasetShardLocation{
		Shard: flowDatasetShardToCmdDatasetShard(shard),
		Host:  proto.String("localhost"),
		Port:  proto.Int32(45327),
	}
}

func getIndexes(task *flow.Task) (indexes []int32) {
	storedValues := task.Step.Params["indexes"].([]int)
	for _, x := range storedValues {
		indexes = append(indexes, int32(x))
	}
	return

}
