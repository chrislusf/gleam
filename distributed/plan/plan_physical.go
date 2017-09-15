package plan

import (
	"log"

	"github.com/chrislusf/gleam/flow"
)

// group local tasks into one task group
func translateToTaskGroups(stepId2StepGroup []*StepGroup) (ret []*TaskGroup) {

	for _, sg := range stepId2StepGroup {
		mustSameNumberOfTasks(sg.Steps)
		count := len(sg.Steps[0].Tasks)
		// println("dealing with", stepGroup.Steps[0].Name, "tasks:", len(stepGroup.Steps[0].Tasks))
		for i := 0; i < count; i++ {
			tg := NewTaskGroup()
			for _, step := range sg.Steps {
				tg.AddTask(step.Tasks[i])
			}
			// depends on the previous step group
			// MAYBE IMPROVEMENT: depends on a subset of previus shards
			tg.ParentStepGroup = sg
			sg.TaskGroups = append(sg.TaskGroups, tg)
			tg.Id = len(ret)
			ret = append(ret, tg)
		}
	}
	return
}

func mustSameNumberOfTasks(steps []*flow.Step) {
	if len(steps) == 0 {
		return
	}
	count := len(steps[0].Tasks)
	for _, step := range steps {
		if count != len(step.Tasks) {
			log.Fatalf("This shouldn't happen: step %d have %d tasks, but step %d have %d tasks.", steps[0].Id, count, step.Id, len(step.Tasks))
		}
	}
}
