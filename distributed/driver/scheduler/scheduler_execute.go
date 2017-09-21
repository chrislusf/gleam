package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func (s *Scheduler) remoteExecuteOnLocation(ctx context.Context,
	flowContext *flow.Flow,
	taskGroupStatus *pb.FlowExecutionStatus_TaskGroup,
	executionStatus *pb.FlowExecutionStatus_TaskGroup_Execution,
	taskGroup *plan.TaskGroup,
	allocation *pb.Allocation, wg *sync.WaitGroup) error {

	// s.setupInputChannels(flowContext, tasks[0], allocation.Location, wg)

	// fmt.Printf("allocated %s on %v\n", tasks[0].Name(), allocation.Location)
	// create reqeust
	instructionSet := plan.TranslateToInstructionSet(taskGroup)
	firstInstruction := instructionSet.GetInstructions()[0]
	lastInstruction := instructionSet.GetInstructions()[len(instructionSet.GetInstructions())-1]
	firstTask := taskGroup.Tasks[0]
	lastTask := taskGroup.Tasks[len(taskGroup.Tasks)-1]
	var inputLocations, outputLocations []pb.DataLocation
	for _, shard := range firstTask.InputShards {
		loc, hasLocation := s.GetShardLocation(shard)
		if !hasLocation {
			log.Printf("The shard is missing?: %s", shard.Name())
			continue
		}
		inputLocations = append(inputLocations, loc)
	}

	for _, shard := range lastTask.OutputShards {
		outputLocations = append(outputLocations, pb.DataLocation{
			Name:     shard.Name(),
			Location: allocation.Location,
			OnDisk:   shard.Dataset.GetIsOnDiskIO(),
		})
	}

	firstInstruction.SetInputLocations(inputLocations)
	lastInstruction.SetOutputLocations(outputLocations)

	instructionSet.FlowHashCode = flowContext.HashCode
	instructionSet.IsProfiling = s.Option.IsProfiling
	instructionSet.Name = taskGroup.String()

	request := &pb.ExecutionRequest{
		InstructionSet: instructionSet,
		Dir:            s.Option.Module,
		Resource:       allocation.Allocated,
	}
	taskGroupStatus.Request = request
	taskGroupStatus.Allocation = allocation

	// println("RequestId:", taskGroup.RequestId, instructions.FlowHashCode)

	if err := sendExecutionRequest(ctx, taskGroupStatus, executionStatus, allocation.Location.URL(), request); err != nil {
		log.Printf("remote execution error: %v", err)
		return err
	}

	return nil
}

func (s *Scheduler) localExecute(ctx context.Context,
	flowContext *flow.Flow,
	executionStatus *pb.FlowExecutionStatus_TaskGroup_Execution,
	task *flow.Task,
	wg *sync.WaitGroup) error {
	if task.Step.OutputDataset == nil {
		return s.localExecuteOutput(ctx, flowContext, task, wg)
	}
	return s.localExecuteSource(ctx, flowContext, executionStatus, task, wg)

}

func (s *Scheduler) localExecuteSource(ctx context.Context,
	flowContext *flow.Flow,
	executionStatus *pb.FlowExecutionStatus_TaskGroup_Execution,
	task *flow.Task,
	wg *sync.WaitGroup) error {
	s.shardLocator.waitForOutputDatasetShardLocations(task)

	instructionStat := &pb.InstructionStat{
		StepId: int32(task.Step.Id),
		TaskId: int32(task.Id),
	}
	executionStatus.ExecutionStat = &pb.ExecutionStat{
		FlowHashCode: flowContext.HashCode,
		Stats:        []*pb.InstructionStat{instructionStat},
	}

	for _, shard := range task.OutputShards {
		location, _ := s.GetShardLocation(shard)
		shard.IncomingChan = util.NewPiper()
		wg.Add(1)
		go func(shard *flow.DatasetShard) {
			// println(task.Step.Name, "writing to", shard.Name(), "at", location.Location.URL())
			if err := netchan.DialWriteChannel(ctx, wg, "driver_input", location.Location.URL(), shard.Name(), shard.Dataset.GetIsOnDiskIO(), shard.IncomingChan.Reader, len(shard.ReadingTasks)); err != nil {
				println("starting:", task.Step.Name, "output location:", location.Location.URL(), shard.Name(), "error:", err.Error())
			}
		}(shard)
	}
	task.Stat = instructionStat
	if err := task.Step.RunFunction(task); err != nil {
		return fmt.Errorf("Failed to send source data: %v", err)
	}
	return nil
}

func (s *Scheduler) localExecuteOutput(ctx context.Context, flowContext *flow.Flow, task *flow.Task, wg *sync.WaitGroup) error {
	s.shardLocator.waitForInputDatasetShardLocations(task)

	for i, shard := range task.InputShards {
		location, _ := s.GetShardLocation(shard)
		inChan := task.InputChans[i]
		wg.Add(1)
		go func(shard *flow.DatasetShard) {
			// println(task.Step.Name, "reading from", shard.Name(), "at", location.Location.URL(), "to", inChan, "onDisk", shard.Dataset.GetIsOnDiskIO())
			if err := netchan.DialReadChannel(ctx, wg, "driver_output", location.Location.URL(), shard.Name(), shard.Dataset.GetIsOnDiskIO(), inChan.Writer); err != nil {
				println("starting:", task.Step.Name, "input location:", location.Location.URL(), shard.Name(), "error:", err.Error())
			}
		}(shard)
	}
	if err := task.Step.RunFunction(task); err != nil {
		return fmt.Errorf("Failed to collect output: %v", err)
	}
	return nil
}
