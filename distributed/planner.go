package distributed

import (
	"context"
	"fmt"

	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
)

type DistributedPlanner struct {
}

func Planner() *DistributedPlanner {
	return &DistributedPlanner{}
}

func (o *DistributedPlanner) GetFlowRunner() flow.FlowRunner {
	return o
}

// driver runs on local, controlling all tasks
func (fcd *DistributedPlanner) RunFlowContext(ctx context.Context, fc *flow.Flow) {

	stepGroups, taskGroups := plan.GroupTasks(fc)

	fmt.Println("=== task execution groups ===")
	for _, taskGroup := range taskGroups {
		fmt.Printf("%s\n", taskGroup.String())
		firstTask := taskGroup.Tasks[0]
		lastTask := taskGroup.Tasks[len(taskGroup.Tasks)-1]
		if len(firstTask.Step.InputDatasets) > 0 {
			fmt.Printf("  input:\n")
			for _, shard := range firstTask.InputShards {
				fmt.Printf("    shard: %v\n", shard.Name())
			}
		}
		if lastTask.Step.OutputDataset != nil {
			fmt.Printf("  output:\n")
			for _, shard := range lastTask.OutputShards {
				fmt.Printf("    shard: %v\n", shard.Name())
			}
		}
	}

	fmt.Println("=== step groups ===")
	for i, stepGroup := range stepGroups {
		fmt.Printf("  step group: %d", i)
		if len(stepGroup.Steps) > 0 && stepGroup.Steps[0].OutputDataset != nil {
			fmt.Printf(" partition: %d", len(stepGroup.Steps[0].OutputDataset.Shards))
		}
		fmt.Println()
		for _, step := range stepGroup.Steps {
			fmt.Printf("    step: %s", step.Name)
			if step.OutputDataset != nil {
				fmt.Printf(" size: %d MB", step.OutputDataset.GetTotalSize())
			}
			fmt.Println()
		}
	}

}
