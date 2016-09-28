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
	"github.com/kardianos/osext"
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
	instructions.FlowHashCode = &flowContext.HashCode
	executableFullFileName, _ := osext.Executable()
	request := NewStartRequest(
		executableFullFileName,
		// filepath.Join(".", filepath.Base(os.Args[0])),
		s.Option.Module,
		instructions,
		allocation.Allocated,
		os.Environ(),
		s.Option.DriverHost,
		int32(s.Option.DriverPort),
	)

	status, isOld := s.getRemoteExecutorStatus(*instructions.FlowHashCode)
	if isOld {
		log.Printf("Replacing old request: %v", status)
	}
	status.RequestTime = time.Now()
	status.Allocation = allocation
	status.Request = request
	taskGroup.RequestId = *instructions.FlowHashCode

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

	println("starting source:", task.Step.Name)
	for _, shard := range task.OutputShards {
		location, _ := s.GetShardLocation(shard)
		shard.IncomingChan = make(chan []byte, 16)
		wg.Add(1)
		go func() {
			println(task.Step.Name, "writing to", shard.Name(), "at", location.URL())
			println("local executor going to write to", shard.Name())
			if err := netchan.DialWriteChannel(wg, location.URL(), shard.Name(), shard.IncomingChan); err != nil {
				println("starting:", task.Step.Name, "output location:", location.URL(), shard.Name(), "error:", err.Error())
			}
			println("completed:", task.Step.Name, "writing.")
		}()
	}
	task.Step.Function(task)
	println("completed:", task.Step.Name)
}

func (s *Scheduler) localExecuteOutput(flowContext *flow.FlowContext, task *flow.Task, wg *sync.WaitGroup) {
	s.shardLocator.waitForInputDatasetShardLocations(task)

	println("starting output:", task.Step.Name)
	for _, shard := range task.InputShards {
		shard.OutgoingChans = make([]chan []byte, 0)
		location, _ := s.GetShardLocation(shard)
		outChan := make(chan []byte, 16)
		shard.OutgoingChans = append(shard.OutgoingChans, outChan)
		wg.Add(1)
		go func() {
			println(task.Step.Name, "reading from", shard.Name(), "at", location.URL(), "to", outChan)
			if err := netchan.DialReadChannel(wg, location.URL(), shard.Name(), outChan); err != nil {
				println("starting:", task.Step.Name, "input location:", location.URL(), shard.Name(), "error:", err.Error())
			}
		}()
	}
	// FIXME Need to output channel to the task
	println("input chan:", task.InputShards[0].OutgoingChans[0])
	task.Step.Function(task)
	println("completed:", task.Step.Name)
}
