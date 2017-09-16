// Package driver coordinates distributed execution.
package driver

import (
	"os"
	"os/user"
	"time"

	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
)

func (fcd *FlowDriver) GetTaskGroupStatus(taskGroup *plan.TaskGroup) *pb.FlowExecutionStatus_TaskGroup {
	for _, status := range fcd.status.TaskGroups {
		if len(taskGroup.Tasks) == len(status.TaskIds) {
			if int32(taskGroup.Tasks[0].Id) == status.TaskIds[0] &&
				int32(taskGroup.Tasks[0].Step.Id) == status.StepIds[0] {
				return status
			}
		}
	}
	return nil
}

func (fcd *FlowDriver) logExecutionPlan(fc *flow.Flow) {

	for _, step := range fc.Steps {
		var (
			parentIds       []int32
			taskIds         []int32
			inputDatasetIds []int32
		)

		for _, ds := range step.InputDatasets {
			parentIds = append(parentIds, int32(ds.Step.Id))
			inputDatasetIds = append(inputDatasetIds, int32(ds.Id))
		}
		for _, t := range step.Tasks {
			taskIds = append(taskIds, int32(t.Id))
		}
		outputDatasetId := int32(0)
		if step.OutputDataset != nil {
			outputDatasetId = int32(step.OutputDataset.Id)
		}
		fcd.status.Steps = append(
			fcd.status.Steps,
			&pb.FlowExecutionStatus_Step{
				Id:              int32(step.Id),
				Name:            step.Name,
				ParentIds:       parentIds,
				TaskIds:         taskIds,
				InputDatasetId:  inputDatasetIds,
				OutputDatasetId: outputDatasetId,
			},
		)
	}

	for _, step := range fc.Steps {
		for _, task := range step.Tasks {
			fcd.status.Tasks = append(
				fcd.status.Tasks,
				&pb.FlowExecutionStatus_Task{
					StepId: int32(step.Id),
					Id:     int32(task.Id),
				},
			)
		}
	}

	for _, sg := range fcd.stepGroups {
		var (
			stepIds   []int32
			parentIds []int32
		)
		for _, step := range sg.Steps {
			stepIds = append(stepIds, int32(step.Id))
		}
		for _, parent := range sg.Parents {
			// find the parent step group from all step groups
			for id, stepGroup := range fcd.stepGroups {
				// if the first step is the same
				if parent.Steps[0].Id == stepGroup.Steps[0].Id {
					parentIds = append(parentIds, int32(id))
				}
			}
		}
		fcd.status.StepGroups = append(
			fcd.status.StepGroups,
			&pb.FlowExecutionStatus_StepGroup{
				StepIds:   stepIds,
				ParentIds: parentIds,
			},
		)
	}

	for _, ds := range fc.Datasets {
		for _, shard := range ds.Shards {
			fcd.status.DatasetShards = append(
				fcd.status.DatasetShards,
				&pb.FlowExecutionStatus_DatasetShard{
					DatasetId: int32(ds.Id),
					Id:        int32(shard.Id),
				},
			)
		}
		var stepIds []int32
		for _, step := range ds.ReadingSteps {
			stepIds = append(stepIds, int32(step.Id))
		}

		fcd.status.Datasets = append(
			fcd.status.Datasets,
			&pb.FlowExecutionStatus_Dataset{
				Id:             int32(ds.Id),
				StepId:         int32(ds.Step.Id),
				ReadingStepIds: stepIds,
			},
		)
	}

	for _, tg := range fcd.taskGroups {
		var (
			stepIds []int32
			taskIds []int32
		)
		for _, task := range tg.Tasks {
			stepIds = append(stepIds, int32(task.Step.Id))
			taskIds = append(taskIds, int32(task.Id))
		}
		taskGroupStatus := &pb.FlowExecutionStatus_TaskGroup{
			StepIds: stepIds,
			TaskIds: taskIds,
		}
		fcd.status.TaskGroups = append(
			fcd.status.TaskGroups,
			taskGroupStatus,
		)
	}

	fcd.status.Id = fc.HashCode

	username := ""
	if currentUser, err := user.Current(); err == nil {
		username = currentUser.Username
	}
	hostname, _ := os.Hostname()
	executable, _ := os.Executable()

	fcd.status.Driver = &pb.FlowExecutionStatus_DriverInfo{
		Username:   username,
		Hostname:   hostname,
		Executable: executable,
		StartTime:  time.Now().UnixNano(),
		Name:       fc.Name,
	}

}
