package plan

import (
	"github.com/chrislusf/gleam/flow"
)

func prepareFlowContext(fc *flow.FlowContext) {
	var sinkSteps []*flow.Step
	for _, step := range fc.Steps {
		t := step // required!!!
		if t.IsOnDriverSide && t.OutputDataset == nil {
			sinkSteps = append(sinkSteps, t)
		}
	}

	for _, step := range sinkSteps {
		collectDatasetSizeInfo(step)
	}
}

func collectDatasetSizeInfo(step *flow.Step) {
	currentDatasetTotalSize := 0
	for _, ds := range step.InputDatasets {
		if ds.Meta.TotalSize == 0 {
			collectDatasetSizeInfo(ds.Step)
		}
		currentDatasetTotalSize += ds.Meta.TotalSize
	}
	if step.OutputDataset != nil {
		step.OutputDataset.Meta.TotalSize = currentDatasetTotalSize
		// println(step.Name, "size:", currentDatasetTotalSize)
	}
}
