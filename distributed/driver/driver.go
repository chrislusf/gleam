// Pacakge driver coordinates distributed execution.
package driver

import (
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/chrislusf/gleam/distributed/driver/scheduler"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/rsync"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util/on_interrupt"
)

type DriverOption struct {
	Leader       string
	DataCenter   string
	Rack         string
	TaskMemoryMB int
	FlowBid      float64
	Module       string
	Host         string
	Port         int
}

var driverOption DriverOption

func init() {
	driverOption.Leader = "localhost:45236"
	driverOption.TaskMemoryMB = 64
	driverOption.FlowBid = 100.0
	driverOption.Host = "localhost"
	driverOption.Port = 0

}

type FlowContextDriver struct {
	Option *DriverOption

	stepGroups []*plan.StepGroup
	taskGroups []*plan.TaskGroup
}

func NewFlowContextDriver(option *DriverOption) *FlowContextDriver {
	return &FlowContextDriver{Option: option}
}

// driver runs on local, controlling all tasks
func (fcd *FlowContextDriver) Run(fc *flow.FlowContext) {

	// task fusion to minimize disk IO
	fcd.stepGroups, fcd.taskGroups = plan.GroupTasks(fc)

	// start server to serve files to agents to run exectuors
	rsyncServer, err := rsync.NewRsyncServer(os.Args[0], nil)
	if err != nil {
		log.Fatalf("Failed to start local server: %v", err)
	}
	rsyncServer.StartRsyncServer(fcd.Option.Host + ":" + strconv.Itoa(fcd.Option.Port))

	driverHost := fcd.Option.Host

	// create thes cheduler
	sched := scheduler.NewScheduler(
		fcd.Option.Leader,
		&scheduler.SchedulerOption{
			DataCenter:         fcd.Option.DataCenter,
			Rack:               fcd.Option.Rack,
			TaskMemoryMB:       fcd.Option.TaskMemoryMB,
			DriverHost:         driverHost,
			DriverPort:         rsyncServer.Port,
			Module:             fcd.Option.Module,
			ExecutableFile:     os.Args[0],
			ExecutableFileHash: rsyncServer.ExecutableFileHash(),
		},
	)

	// best effort to clean data on agent disk
	// this may need more improvements
	defer fcd.Cleanup(sched, fc)

	go sched.EventLoop()

	on_interrupt.OnInterrupt(func() {
		fcd.OnInterrupt(fc, sched)
	}, func() {
		fcd.OnExit(fc, sched)
	})

	// schedule to run the steps
	var wg sync.WaitGroup
	for _, taskGroup := range fcd.taskGroups {
		wg.Add(1)
		sched.EventChan <- scheduler.SubmitTaskGroup{
			FlowContext: fc,
			TaskGroup:   taskGroup,
			Bid:         fcd.Option.FlowBid / float64(len(fcd.taskGroups)),
			WaitGroup:   &wg,
		}
	}
	go sched.Market.FetcherLoop()

	wg.Wait()

}

func (fcd *FlowContextDriver) Cleanup(sched *scheduler.Scheduler, fc *flow.FlowContext) {
	var wg sync.WaitGroup
	wg.Add(1)
	sched.EventChan <- scheduler.ReleaseTaskGroupInputs{
		FlowContext: fc,
		TaskGroups:  fcd.taskGroups,
		WaitGroup:   &wg,
	}

	wg.Wait()
}
