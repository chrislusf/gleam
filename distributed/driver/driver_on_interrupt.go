package driver

import (
	"fmt"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler"
)

func (fcd *FlowContextDriver) printDistributedStatus(sched *scheduler.Scheduler) {
	fmt.Print("\n")
	for _, stepGroup := range fcd.stepGroups {
		fmt.Print("step:")
		for _, step := range stepGroup.Steps {
			fmt.Printf(" %s%d", step.Name, step.Id)
		}
		fmt.Print("\n")

		for _, tg := range stepGroup.TaskGroups {
			statusTaskGroup := fcd.GetTaskGroupStatus(tg)
			for _, stat := range statusTaskGroup.Executions {
				host := statusTaskGroup.Request.GetHost()
				port := statusTaskGroup.Request.GetPort()
				timeTaken := time.Now().Sub(time.Unix(stat.StartTime, 0))
				taskGroupName := statusTaskGroup.GetRequest().GetName()
				if stat.GetStopTime() == 0 {
					fmt.Printf("  %s:%d tasks:%s took time:%v\n", host, port, taskGroupName, timeTaken)
				} else if stat.GetError() != nil {
					fmt.Printf("  %s:%d tasks:%s has error:%v\n", host, port, string(stat.GetError()))
				}
			}
		}

	}
	fmt.Print("\n")
}
