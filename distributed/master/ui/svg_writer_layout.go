package ui

import (
	"github.com/chrislusf/gleam/pb"
)

// separate step groups into layers of step group ids via depencency analysis
func toStepGroupLayers(status *pb.FlowExecutionStatus) (layers [][]int) {

	// how many step groups depenend on this step group
	dependencyCount := make([]int, len(status.StepGroups))
	isUsed := make([]bool, len(status.StepGroups))
	for _, stepGroup := range status.StepGroups {
		for _, parentId := range stepGroup.GetParentIds() {
			dependencyCount[parentId]++
		}
	}

	noDependencyStepGroupIds := checkAllDepencies(dependencyCount, isUsed)

	for len(noDependencyStepGroupIds) > 0 {
		layers = append(layers, noDependencyStepGroupIds)

		// maintain dependencyCount after one layer
		for _, stepGroupId := range noDependencyStepGroupIds {
			for _, parentId := range status.StepGroups[stepGroupId].GetParentIds() {
				dependencyCount[parentId]--
			}
		}
		noDependencyStepGroupIds = checkAllDepencies(dependencyCount, isUsed)
	}

	return

}

func checkAllDepencies(dep []int, isUsed []bool) (noDependencyStepGroupIds []int) {
	for id := 0; id < len(dep); id++ {
		if dep[id] == 0 && !isUsed[id] {
			noDependencyStepGroupIds = append(noDependencyStepGroupIds, id)
			isUsed[id] = true
		}
	}
	return
}
