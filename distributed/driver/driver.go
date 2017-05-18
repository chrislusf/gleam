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
}

type FlowContextDriver struct {
	Option *Option

	stepGroups []*plan.StepGroup
	taskGroups []*plan.TaskGroup

	status *pb.FlowExecutionStatus
}

func NewFlowContextDriver(option *Option) *FlowContextDriver {
	return &FlowContextDriver{
		Option: option,
		status: &pb.FlowExecutionStatus{},
	}
}

// driver runs on local, controlling all tasks
func (fcd *FlowContextDriver) RunFlowContext(fc *flow.FlowContext) {

	// task fusion to minimize disk IO
	fcd.stepGroups, fcd.taskGroups = plan.GroupTasks(fc)
	fcd.logExecutionPlan(fc)

	// create thes cheduler
	sched := scheduler.NewScheduler(
		fcd.Option.Master,
		&scheduler.SchedulerOption{
			DataCenter:   fcd.Option.DataCenter,
			Rack:         fcd.Option.Rack,
			TaskMemoryMB: fcd.Option.TaskMemoryMB,
			Module:       fcd.Option.Module,
			FlowHashcode: fc.HashCode,
		},
	)

	// best effort to clean data on agent disk
	// this may need more improvements
	defer fcd.cleanup(sched, fc)

	ctx, cancel := context.WithCancel(context.Background())

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

	log.Printf("Job Status URL http://%s/job/%d", fcd.Option.Master, fcd.status.GetId())

	wg.Wait()

	stopChan <- true
	reportWg.Wait()

}

func (fcd *FlowContextDriver) cleanup(sched *scheduler.Scheduler, fc *flow.FlowContext) {
	var wg sync.WaitGroup

	for _, taskGroup := range fcd.taskGroups {
		wg.Add(1)
		go func(taskGroup *plan.TaskGroup) {
			defer wg.Done()
			sched.DeleteOutout(taskGroup)
		}(taskGroup)
	}

	wg.Wait()
}

func (fcd *FlowContextDriver) reportStatus(ctx context.Context, wg *sync.WaitGroup, master string, stopChan chan bool) {
	grpcConection, err := grpc.Dial(master, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to dial: %v", err)
		return
	}
	defer func() {
		// println("grpc closing....")

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
				log.Printf("Job Status URL http://%s/job/%d", fcd.Option.Master, fcd.status.GetId())
			} else {
				log.Printf("Failed to update Job Status http://%s/job/%d : %v", fcd.Option.Master, fcd.status.GetId(), err)
			}
			// log.Printf("Sent last status: %v", fcd.status)
			stream.CloseSend()
			return
		case <-ticker.C:
			stream.Send(fcd.status)
		}
	}

}
