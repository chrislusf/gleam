package plan

import (
	"log"

	"github.com/chrislusf/gleam/flow"
)

func isMergeableDataset(ds *flow.Dataset, taskCount int) bool {
	if taskCount != len(ds.Shards) {
		return false
	}
	if taskCount != len(ds.Step.Tasks) {
		return false
	}
	if len(ds.ReadingSteps) > 1 {
		return false
	}
	for _, shard := range ds.Shards {
		if len(shard.ReadingTasks) > 1 {
			return false
		}
	}
	return true
}

// find mergeable parent step or itself if parent is not mergeable
func findAncestorStepId(step *flow.Step) (int, bool) {
	current := step
	taskCount := len(current.Tasks)

	// println("find step", step.Name)

	for taskCount == len(current.Tasks) {
		if len(current.InputDatasets) > 1 {
			// more than 2 dataset inputs
			break
		}
		if len(current.InputDatasets) == 0 {
			// no dataset inputs
			break
		}
		if !isMergeableDataset(current.InputDatasets[0], taskCount) {
			break
		}
		if (!current.IsOnDriverSide && current.InputDatasets[0].Step.IsOnDriverSide) ||
			(current.IsOnDriverSide && !current.InputDatasets[0].Step.IsOnDriverSide) {
			break
		}

		current = current.InputDatasets[0].Step
		taskCount = len(current.Tasks)

	}
	return current.Id, true
}

// group local steps into one step group
func translateToStepGroups(fc *flow.Flow) []*StepGroup {
	// use array instead of map to ensure consistent ordering
	stepId2StepGroup := make([]*StepGroup, len(fc.Steps))
	for _, step := range fc.Steps {
		// println("step:", step.Name, step.Id, "starting...")
		ancestorStepId, foundStepId := findAncestorStepId(step)
		if !foundStepId {
			println("step:", step.Id, "Not found ancestorStepId.")
			continue
		}
		// println("step:", step.Name, step.Id, "ancestorStepId", ancestorStepId)
		if stepId2StepGroup[ancestorStepId] == nil {
			stepId2StepGroup[ancestorStepId] = NewStepGroup()
			for _, ds := range step.InputDatasets {
				parentDsId, hasParentIdId := findAncestorStepId(ds.Step)
				if !hasParentIdId {
					// since we add steps following the same order as the code
					log.Panic("parent StepGroup should already be in the map")
				}
				parentSg := stepId2StepGroup[parentDsId]
				if parentSg == nil {
					// since we add steps following the same order as the code
					log.Panic("parent StepGroup should already be in the map")
				}
				stepId2StepGroup[ancestorStepId].AddParent(parentSg)
			}
		}
		stepId2StepGroup[ancestorStepId].AddStep(step)
	}
	// shrink
	var ret []*StepGroup
	for _, stepGroup := range stepId2StepGroup {
		if stepGroup == nil || len(stepGroup.Steps) == 0 {
			continue
		}
		// println("add step group started by", stepGroup.Steps[0].Name, "with", len(stepGroup.Steps), "steps")
		ret = append(ret, stepGroup)
	}

	return ret
}
