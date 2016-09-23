package driver

import (
	"fmt"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func (fcd *FlowContextDriver) ShowFlowStatus(
	fc *flow.FlowContext,
	sched *scheduler.Scheduler) {
	status := fcd.collectStatusFromRemoteExecutors(sched)
	fcd.printDistributedStatus(sched, status)
}

func (fcd *FlowContextDriver) OnInterrupt(
	fc *flow.FlowContext,
	sched *scheduler.Scheduler) {
	fcd.ShowFlowStatus(fc, sched)
}

func (fcd *FlowContextDriver) OnExit(
	fc *flow.FlowContext,
	sched *scheduler.Scheduler) {
	var wg sync.WaitGroup
	for _, tg := range fcd.taskGroups {
		wg.Add(1)
		go func(tg *plan.TaskGroup) {
			defer wg.Done()

			requestId := tg.RequestId
			request, ok := sched.RemoteExecutorStatuses[requestId]
			if !ok {
				fmt.Printf("No executors for %v\n", tg)
				return
			}
			// println("checking", request.Allocation.Location.URL(), requestId)
			if err := askExecutorToStopRequest(request.Allocation.Location.URL(), requestId); err != nil {
				fmt.Printf("Error to stop request %d on %s: %v\n", requestId, request.Allocation.Location.URL(), err)
				return
			}
		}(tg)
	}
	wg.Wait()

}

func (fcd *FlowContextDriver) printDistributedStatus(sched *scheduler.Scheduler, stats []*RemoteExecutorStatus) {
	fmt.Print("\n")
	for _, stepGroup := range fcd.stepGroups {
		fmt.Print("step:")
		for _, step := range stepGroup.Steps {
			fmt.Printf(" %s%d", step.Name, step.Id)
		}
		fmt.Print("\n")

		for _, tg := range stepGroup.TaskGroups {
			stat := stats[tg.Id]
			firstTask := tg.Tasks[0]
			lastTask := tg.Tasks[len(tg.Tasks)-1]
			if stat == nil {
				fmt.Printf("  No status.\n")
				continue
			}
			if stat.IsClosed() {
				fmt.Printf("  %s task:%s took time:%v\n", stat.Allocation.Location.URL(), lastTask.Step.Name, stat.TimeTaken())
			} else {
				fmt.Printf("  %s first task:%s time:%v\n", stat.Allocation.Location.URL(), firstTask.Step.Name, stat.TimeTaken())
			}
			for _, inputStat := range stat.InputChannelStatuses {
				fmt.Printf("    input  : %s  length:%d\n", inputStat.Name, inputStat.Length)
			}
			for _, outputStat := range stat.OutputChannelStatuses {
				fmt.Printf("    output : %s  length:%d\n", outputStat.Name, outputStat.Length)
			}
		}

	}
	fmt.Print("\n")
}

func (fcd *FlowContextDriver) collectStatusFromRemoteExecutors(sched *scheduler.Scheduler) []*RemoteExecutorStatus {
	stats := make([]*RemoteExecutorStatus, len(fcd.taskGroups))
	var wg sync.WaitGroup
	for _, tg := range fcd.taskGroups {
		wg.Add(1)
		go func(tg *plan.TaskGroup) {
			defer wg.Done()

			requestId := tg.RequestId
			request, ok := sched.RemoteExecutorStatuses[requestId]
			if !ok {
				fmt.Printf("No executors for %v\n", tg)
				return
			}
			// println("checking", request.Allocation.Location.URL(), requestId)
			stat, err := askExecutorStatusForRequest(request.Allocation.Location.URL(), requestId)
			if err != nil {
				fmt.Printf("Error to request status from %s: %v\n", request.Allocation.Location.URL(), err)
				return
			}
			// println("back from", request.Allocation.Location.URL(), requestId)
			stat.Allocation = request.Allocation
			stat.taskGroup = tg
			stats[tg.Id] = stat
		}(tg)
	}
	wg.Wait()
	return stats
}

func askExecutorStatusForRequest(server string, requestId uint32) (*RemoteExecutorStatus, error) {

	reply, err := scheduler.RemoteDirectCommand(server, scheduler.NewGetStatusRequest(requestId))
	if err != nil {
		return nil, err
	}

	response := reply.GetGetStatusResponse()

	return &RemoteExecutorStatus{
		ExecutorStatus: util.ExecutorStatus{
			InputChannelStatuses:  FromProto(response.GetInputStatuses()),
			OutputChannelStatuses: FromProto(response.GetOutputStatuses()),
			RequestTime:           time.Unix(response.GetRequestTime(), 0),
			StartTime:             time.Unix(response.GetStartTime(), 0),
			StopTime:              time.Unix(response.GetStopTime(), 0),
		},
	}, nil
}

func askExecutorToStopRequest(server string, requestId uint32) (err error) {
	_, err = scheduler.RemoteDirectCommand(server, scheduler.NewStopRequest(requestId))
	return
}
