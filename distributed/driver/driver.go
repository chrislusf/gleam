// Package driver coordinates distributed execution.
package driver

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util/on_interrupt"
	"google.golang.org/grpc"
)

type Option struct {
	RequiredFiles []resource.FileResource
	Master        string
	DataCenter    string
	Rack          string
	TaskMemoryMB  int
	FlowBid       float64
	Module        string
	IsProfiling   bool
}

type FlowDriver struct {
	Option *Option

	stepGroups []*plan.StepGroup
	taskGroups []*plan.TaskGroup

	status *pb.FlowExecutionStatus
}

func NewFlowDriver(option *Option) *FlowDriver {
	return &FlowDriver{
		Option: option,
		status: &pb.FlowExecutionStatus{},
	}
}

// driver runs on local, controlling all tasks
func (fcd *FlowDriver) RunFlowContext(parentCtx context.Context, fc *flow.Flow) {

	// task fusion to minimize disk IO
	fcd.stepGroups, fcd.taskGroups = plan.GroupTasks(fc)
	fcd.logExecutionPlan(fc)

	// create the scheduler
	sched := scheduler.New(
		fcd.Option.Master,
		&scheduler.Option{
			DataCenter:   fcd.Option.DataCenter,
			Rack:         fcd.Option.Rack,
			TaskMemoryMB: fcd.Option.TaskMemoryMB,
			Module:       fcd.Option.Module,
			FlowHashcode: fc.HashCode,
			IsProfiling:  fcd.Option.IsProfiling,
		},
	)

	// best effort to clean data on agent disk
	// this may need more improvements
	defer fcd.cleanup(sched, fc)

	ctx, cancel := context.WithCancel(parentCtx)

	on_interrupt.OnInterrupt(func() {
		println("interrupted ...")
		fcd.printDistributedStatus(os.Stderr)
		cancel()
		fcd.cleanup(sched, fc)
	}, nil)

	// schedule to run the steps
	var wg, reportWg sync.WaitGroup
	for _, taskGroup := range fcd.taskGroups {
		wg.Add(1)
		go func(taskGroup *plan.TaskGroup) {
			sched.ExecuteTaskGroup(ctx, fc, fcd.GetTaskGroupStatus(taskGroup), &wg, taskGroup,
				fcd.Option.FlowBid/float64(len(fcd.taskGroups)), fcd.Option.RequiredFiles)
		}(taskGroup)
	}
	go sched.Market.FetcherLoop()

	stopChan := make(chan bool)
	reportWg.Add(1)
	go fcd.reportStatus(ctx, &reportWg, fcd.Option.Master, stopChan)

	log.Printf("Start Job Status URL http://%s/job/%d", fcd.Option.Master, fcd.status.GetId())

	wg.Wait()

	stopChan <- true
	reportWg.Wait()

}

func (fcd *FlowDriver) cleanup(sched *scheduler.Scheduler, fc *flow.Flow) {
	var wg sync.WaitGroup

	for _, taskGroup := range fcd.taskGroups {
		wg.Add(1)
		go func(taskGroup *plan.TaskGroup) {
			defer wg.Done()
			sched.DeleteOutput(taskGroup)
		}(taskGroup)
	}

	wg.Wait()

	if fcd.Option.IsProfiling {
		// TODO send the pprof files back to driver
		return
	}

	touchedAgents := make(map[string]bool)
	for _, taskGroup := range fcd.taskGroups {
		tasks := taskGroup.Tasks
		for _, shard := range tasks[len(tasks)-1].OutputShards {
			location, _ := sched.GetShardLocation(shard)
			if location.Location == nil {
				continue
			}
			touchedAgents[location.Location.URL()] = true
		}
	}

	for url, _ := range touchedAgents {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			if err := scheduler.SendCleanupRequest(url, &pb.CleanupRequest{
				FlowHashCode: fc.HashCode,
			}); err != nil {
				println("Purging dataset error:", err.Error())
			}
		}(url)
	}

	wg.Wait()
}

func (fcd *FlowDriver) reportStatus(ctx context.Context, wg *sync.WaitGroup, master string, stopChan chan bool) {
	grpcConection, err := grpc.Dial(master, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to dial: %v", err)
		return
	}
	defer func() {
		// println("grpc closing....")
		time.Sleep(50 * time.Millisecond)
		if err := grpcConection.Close(); err != nil {
			log.Printf("grpcConection.close error: %v", err)
		}

		wg.Done()
	}()
	client := pb.NewGleamMasterClient(grpcConection)

	stream, err := client.SendFlowExecutionStatus(ctx)
	if err != nil {
		log.Printf("Failed to create stream on SendFlowExecutionStatus: %v", err)
		return
	}

	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-stopChan:
			fcd.status.Driver.StopTime = time.Now().UnixNano()
			if err = stream.Send(fcd.status); err == nil {
				log.Printf("Saved Job Status URL http://%s/job/%d", fcd.Option.Master, fcd.status.GetId())
			} else {
				log.Printf("Failed to update Job Status http://%s/job/%d : %v", fcd.Option.Master, fcd.status.GetId(), err)
			}
			stream.CloseSend()
			return
		case <-ticker.C:
			stream.Send(fcd.status)
		}
	}

}
