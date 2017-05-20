package driver

import (
	"fmt"
	"io"
	"time"
)

func (fcd *FlowDriver) printDistributedStatus(writer io.Writer) {
	for _, stepGroup := range fcd.stepGroups {
		fmt.Fprint(writer, "step:")
		for _, step := range stepGroup.Steps {
			fmt.Fprintf(writer, " %s %d", step.Name, step.Id)
		}
		fmt.Fprint(writer, "\n")

		for _, tg := range stepGroup.TaskGroups {
			statusTaskGroup := fcd.GetTaskGroupStatus(tg)
			for _, stat := range statusTaskGroup.Executions {
				taskGroupName := statusTaskGroup.GetRequest().GetInstructionSet().GetName()
				fmt.Fprintf(writer, "  tasks:%s ", taskGroupName)
				if stat.GetStopTime() == 0 {
					timeTaken := time.Now().Sub(time.Unix(stat.StartTime/1e9, stat.StartTime%1e9))
					fmt.Fprintf(writer, "took time:%v\n", timeTaken)
				} else if stat.GetError() != nil {
					timeTaken := time.Duration(stat.StopTime - stat.StartTime)
					fmt.Fprintf(writer, "runs in %v, but has error:%v\n", timeTaken, string(stat.GetError()))
				} else {
					timeTaken := time.Duration(stat.StopTime - stat.StartTime)
					fmt.Fprintf(writer, "completed in %v\n", timeTaken)
				}
			}
		}

	}
	fmt.Fprint(writer, "\n")
}
