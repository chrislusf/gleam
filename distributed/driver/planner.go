package driver

import (
	"fmt"

	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
)

type FlowContextPlanner struct {
}

func NewFlowContextPlanner() *FlowContextPlanner {
	return &FlowContextPlanner{}
}

// driver runs on local, controlling all tasks
func (fcd *FlowContextPlanner) Run(fc *flow.FlowContext) {

	stepGroups, taskGroups := plan.GroupTasks(fc)

	fmt.Println("=== task execution groups ===")
	for _, taskGroup := range taskGroups {
		fmt.Printf("%s\n", taskGroup.String())
		firstTask := taskGroup.Tasks[0]
		lastTask := taskGroup.Tasks[len(taskGroup.Tasks)-1]
		if len(firstTask.Step.InputDatasets) > 0 {
			for _, ds := range firstTask.Step.InputDatasets {
				fmt.Printf("  input:  dataset %v\n", ds.Id)
				for _, shard := range ds.Shards {
					fmt.Printf("    shard: %v\n", shard.Name())
				}
			}
		}
		if lastTask.Step.OutputDataset != nil {
			fmt.Printf("  output: dataset %v\n", lastTask.Step.OutputDataset.Id)
			for _, shard := range lastTask.Step.OutputDataset.Shards {
				fmt.Printf("    shard: %v\n", shard.Name())
			}
		}
	}

	fmt.Println("=== step groups ===")
	for i, stepGroup := range stepGroups {
		fmt.Printf("  step: %d\n", i)
		for _, step := range stepGroup.Steps {
			fmt.Printf("    step: %s\n", step.Name)
		}
	}

}
