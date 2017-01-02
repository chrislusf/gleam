package scheduler

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func (s *Scheduler) remoteExecuteOnLocation(ctx context.Context,
	flowContext *flow.FlowContext,
	statusTaskGroup *pb.FlowExecutionStatus_TaskGroup,
	taskGroup *plan.TaskGroup,
	allocation *pb.Allocation, wg *sync.WaitGroup) error {

	// s.setupInputChannels(flowContext, tasks[0], allocation.Location, wg)

	// fmt.Printf("allocated %s on %v\n", tasks[0].Name(), allocation.Location)
	// create reqeust
	args := []string{}
	for _, arg := range os.Args[1:] {
		args = append(args, arg)
	}
	instructions := plan.TranslateToInstructionSet(taskGroup)
	firstInstruction := instructions.GetInstructions()[0]
	lastInstruction := instructions.GetInstructions()[len(instructions.GetInstructions())-1]
	firstTask := taskGroup.Tasks[0]
	lastTask := taskGroup.Tasks[len(taskGroup.Tasks)-1]
	var inputLocations, outputLocations []pb.DataLocation
	for _, shard := range firstTask.InputShards {
		loc, hasLocation := s.getShardLocation(shard)
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

	instructions.FlowHashCode = flowContext.HashCode
	instructions.IsProfiling = false // enable this when profiling executors

	request := &pb.ExecutionRequest{
		Instructions: instructions,
		Files:        nil,
		Dir:          s.Option.Module,
		Name:         taskGroup.String(),
		Resource:     allocation.Allocated,
		Host:         s.Option.DriverHost,
		Port:         int32(s.Option.DriverPort),
	}
	statusTaskGroup.Request = request

	statusExecution := &pb.FlowExecutionStatus_TaskGroup_Execution{}
	statusTaskGroup.Executions = append(statusTaskGroup.Executions, statusExecution)
	statusExecution.StartTime = time.Now().Unix()
	defer func() {
		statusExecution.StopTime = time.Now().Unix()
	}()

	// println("RequestId:", taskGroup.RequestId, instructions.FlowHashCode)

	if err := sendExecutionRequest(ctx, statusExecution, allocation.Location.URL(), request); err != nil {
		log.Printf("remote execution error: %v", err)
		statusExecution.Error = []byte(err.Error())
		return err
	}

	return nil
}

func (s *Scheduler) localExecute(ctx context.Context, flowContext *flow.FlowContext, task *flow.Task, wg *sync.WaitGroup) {
	if task.Step.OutputDataset == nil {
		s.localExecuteOutput(ctx, flowContext, task, wg)
	} else {
		s.localExecuteSource(ctx, flowContext, task, wg)
	}
}

func (s *Scheduler) localExecuteSource(ctx context.Context, flowContext *flow.FlowContext, task *flow.Task, wg *sync.WaitGroup) {
	s.shardLocator.waitForOutputDatasetShardLocations(task)

	for _, shard := range task.OutputShards {
		location, _ := s.getShardLocation(shard)
		shard.IncomingChan = util.NewPiper()
		wg.Add(1)
		go func(shard *flow.DatasetShard) {
			// println(task.Step.Name, "writing to", shard.Name(), "at", location.URL())
			if err := netchan.DialWriteChannel(ctx, wg, "driver_input", location.Location.URL(), shard.Name(), shard.Dataset.GetIsOnDiskIO(), shard.IncomingChan.Reader, len(shard.ReadingTasks)); err != nil {
				println("starting:", task.Step.Name, "output location:", location.Location.URL(), shard.Name(), "error:", err.Error())
			}
		}(shard)
	}
	if err := task.Step.RunFunction(task); err != nil {
		log.Fatalf("Failed to send source data: %v", err)
	}
}

func (s *Scheduler) localExecuteOutput(ctx context.Context, flowContext *flow.FlowContext, task *flow.Task, wg *sync.WaitGroup) {
	s.shardLocator.waitForInputDatasetShardLocations(task)

	for i, shard := range task.InputShards {
		location, _ := s.getShardLocation(shard)
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
		log.Fatalf("Failed to collect output: %v", err)
	}
}
