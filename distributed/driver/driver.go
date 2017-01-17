// Package driver coordinates distributed execution.
package driver

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/chrislusf/gleam/distributed/driver/scheduler"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/rsync"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util/on_interrupt"
)

type Option struct {
	RequiredFiles []rsync.FileResource
	Master        string
	DataCenter    string
	Rack          string
	TaskMemoryMB  int
	FlowBid       float64
	Module        string
	Host          string
	Port          int
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

	// start server to serve files to agents to run exectuors
	rsyncServer, err := rsync.NewRsyncServer(fcd.Option.RequiredFiles...)
	if err != nil {
		log.Fatalf("Failed to start local server: %v", err)
	}
	rsyncServer.StartRsyncServer(fcd.Option.Host + ":" + strconv.Itoa(fcd.Option.Port))

	// create thes cheduler
	sched := scheduler.NewScheduler(
		fcd.Option.Master,
		&scheduler.SchedulerOption{
			DataCenter:   fcd.Option.DataCenter,
			Rack:         fcd.Option.Rack,
			TaskMemoryMB: fcd.Option.TaskMemoryMB,
			DriverHost:   fcd.Option.Host,
			DriverPort:   rsyncServer.Port,
			Module:       fcd.Option.Module,
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
	var wg sync.WaitGroup
	for _, taskGroup := range fcd.taskGroups {
		wg.Add(1)
		go sched.ExecuteTaskGroup(ctx, fc, fcd.GetTaskGroupStatus(taskGroup), &wg, taskGroup,
			fcd.Option.FlowBid/float64(len(fcd.taskGroups)))
	}
	go sched.Market.FetcherLoop()

	wg.Wait()

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
