package scheduler

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/netchan"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
)

func (s *Scheduler) remoteExecuteOnLocation(flowContext *flow.FlowContext, taskGroup *plan.TaskGroup, allocation resource.Allocation, wg *sync.WaitGroup) {
	// s.setupInputChannels(flowContext, tasks[0], allocation.Location, wg)

	// fmt.Printf("allocated %s on %v\n", tasks[0].Name(), allocation.Location)
	// create reqeust
	args := []string{}
	for _, arg := range os.Args[1:] {
		args = append(args, arg)
	}
	instructions := plan.TranslateToInstructionSet(taskGroup)
	firstInstruction := instructions.GetInstructions()[0]
	firstTask := taskGroup.Tasks[0]
	var inputLocations []resource.Location
	for _, shard := range firstTask.InputShards {
		loc, hasLocation := s.GetShardLocation(shard)
		if !hasLocation {
			log.Printf("The shard is missing?: %s", shard.Name())
			continue
		}
		inputLocations = append(inputLocations, loc)
	}
	firstInstruction.SetInputLocations(inputLocations...)

	instructions.FlowHashCode = &flowContext.HashCode
	request := NewStartRequest(
		s.Option.Module,
		instructions,
		allocation.Allocated,
		os.Environ(),
		s.Option.DriverHost,
		int32(s.Option.DriverPort),
	)

	status, isOld := s.getRemoteExecutorStatus(instructions.HashCode())
	if isOld {
		log.Printf("Replacing old request: %v", status)
	}
	status.RequestTime = time.Now()
	status.Allocation = allocation
	status.Request = request
	taskGroup.RequestId = instructions.HashCode()

	// fmt.Printf("starting on %s: %v\n", allocation.Allocated, request)

	if err := RemoteDirectExecute(allocation.Location.URL(), request); err != nil {
		log.Printf("remote exeuction error %v: %v", err, request)
	}
	status.StopTime = time.Now()
}

func (s *Scheduler) localExecute(flowContext *flow.FlowContext, task *flow.Task, wg *sync.WaitGroup) {
	if task.Step.OutputDataset == nil {
		s.localExecuteOutput(flowContext, task, wg)
	} else {
		s.localExecuteSource(flowContext, task, wg)
	}
}

func (s *Scheduler) localExecuteSource(flowContext *flow.FlowContext, task *flow.Task, wg *sync.WaitGroup) {
	s.shardLocator.waitForOutputDatasetShardLocations(task)

	for _, shard := range task.OutputShards {
		location, _ := s.GetShardLocation(shard)
		shard.IncomingChan = make(chan []byte, 16)
		wg.Add(1)
		go func() {
			if err := netchan.DialWriteChannel(wg, location.URL(), shard.Name(), shard.IncomingChan); err != nil {
				println("starting:", task.Step.Name, "output location:", location.URL(), shard.Name(), "error:", err.Error())
			}
		}()
	}
	task.Step.Function(task)
}

func (s *Scheduler) localExecuteOutput(flowContext *flow.FlowContext, task *flow.Task, wg *sync.WaitGroup) {
	s.shardLocator.waitForInputDatasetShardLocations(task)

	for _, shard := range task.InputShards {
		shard.OutgoingChans = make([]chan []byte, 0)
		location, _ := s.GetShardLocation(shard)
		outChan := make(chan []byte, 16)
		shard.OutgoingChans = append(shard.OutgoingChans, outChan)
		wg.Add(1)
		go func() {
			// println(task.Step.Name, "reading from", shard.Name(), "at", location.URL(), "to", outChan)
			if err := netchan.DialReadChannel(wg, location.URL(), shard.Name(), outChan); err != nil {
				println("starting:", task.Step.Name, "input location:", location.URL(), shard.Name(), "error:", err.Error())
			}
		}()
	}
	task.Step.Function(task)
}
