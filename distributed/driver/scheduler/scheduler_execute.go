package scheduler

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
)

func (s *Scheduler) remoteExecuteOnLocation(flowContext *flow.FlowContext, taskGroup *plan.TaskGroup, allocation resource.Allocation, wg *sync.WaitGroup) {
	tasks := taskGroup.Tasks

	// s.setupInputChannels(flowContext, tasks[0], allocation.Location, wg)

	for _, shard := range tasks[len(tasks)-1].OutputShards {
		s.SetShardLocation(shard, allocation.Location)
	}

	// fmt.Printf("allocated %s on %v\n", tasks[0].Name(), allocation.Location)
	// create reqeust
	args := []string{}
	for _, arg := range os.Args[1:] {
		args = append(args, arg)
	}
	request := NewStartRequest(
		"./"+filepath.Base(os.Args[0]),
		// filepath.Join(".", filepath.Base(os.Args[0])),
		s.Option.Module,
		plan.TranslateToInstructionSet(taskGroup),
		allocation.Allocated,
		os.Environ(),
		s.Option.DriverHost,
		int32(s.Option.DriverPort),
	)

	requestId := request.StartRequest.GetHashCode()
	status, isOld := s.getRemoteExecutorStatus(requestId)
	if isOld {
		log.Printf("Replacing old request: %v", status)
	}
	status.RequestTime = time.Now()
	status.Allocation = allocation
	status.Request = request
	taskGroup.RequestId = requestId

	// fmt.Printf("starting on %s: %v\n", allocation.Allocated, request)
	if err := RemoteDirectExecute(allocation.Location.URL(), request); err != nil {
		log.Printf("remote exeuction error %v: %v", err, request)
	}
	status.StopTime = time.Now()
}
