package plan

import (
	"fmt"
	"log"
	"strings"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
)

type TaskGroup struct {
	Id              int
	Tasks           []*flow.Task
	Parents         []*TaskGroup
	ParentStepGroup *StepGroup
	RequestId       uint32 // id for actual request when running
}

type StepGroup struct {
	Steps      []*flow.Step
	Parents    []*StepGroup
	TaskGroups []*TaskGroup
}

func GroupTasks(fc *flow.FlowContext) ([]*StepGroup, []*TaskGroup) {
	stepGroups := translateToStepGroups(fc)
	return stepGroups, translateToTaskGroups(stepGroups)
}

func NewStepGroup() *StepGroup {
	return &StepGroup{}
}

func (t *StepGroup) AddStep(Step *flow.Step) *StepGroup {
	t.Steps = append(t.Steps, Step)
	return t
}

func (t *StepGroup) AddParent(parent *StepGroup) *StepGroup {
	t.Parents = append(t.Parents, parent)
	return t
}

func NewTaskGroup() *TaskGroup {
	return &TaskGroup{}
}

func (t *TaskGroup) AddTask(task *flow.Task) *TaskGroup {
	t.Tasks = append(t.Tasks, task)
	return t
}

func (t *TaskGroup) AddParent(parent *TaskGroup) *TaskGroup {
	t.Parents = append(t.Parents, parent)
	return t
}

func (t *TaskGroup) String() string {
	var steps []string
	for _, task := range t.Tasks {
		steps = append(steps, fmt.Sprintf("%s.%d", task.Step.Name, task.Id))
	}
	return "taskGroup:" + strings.Join(steps, "-")
}

func (t *TaskGroup) RequiredResources() resource.ComputeResource {

	resource := resource.ComputeResource{
		CPUCount: 1,
		CPULevel: 1,
	}

	for _, task := range t.Tasks {
		inst := task.Step.Instruction
		if inst != nil && task.Step.OutputDataset != nil {
			taskMemSize := inst.GetMemoryCostInMB(task.Step.OutputDataset.GetPartitionSize())
			resource.MemoryMB += taskMemSize
			log.Printf("  %s : %s (%d MB)\n", t.String(), task.Step.Name, taskMemSize)
		}
	}

	return resource
}
