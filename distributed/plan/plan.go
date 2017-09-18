package plan

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
)

type TaskGroup struct {
	Id              int
	Tasks           []*flow.Task
	Parents         []*TaskGroup
	ParentStepGroup *StepGroup
	RequestId       uint32 // id for actual request when running
	WaitAt          time.Time
	StartAt         time.Time
	StopAt          time.Time
	Error           error
}

type StepGroup struct {
	Steps      []*flow.Step
	Parents    []*StepGroup
	TaskGroups []*TaskGroup
	sync.Mutex
	waitForAllTasks *sync.Cond
}

func GroupTasks(fc *flow.Flow) ([]*StepGroup, []*TaskGroup) {
	stepGroups := translateToStepGroups(fc)
	return stepGroups, translateToTaskGroups(stepGroups)
}

func NewStepGroup() *StepGroup {
	sg := &StepGroup{}
	sg.waitForAllTasks = sync.NewCond(sg)
	return sg
}

func (s *StepGroup) AddStep(Step *flow.Step) *StepGroup {
	s.Steps = append(s.Steps, Step)
	return s
}

func (s *StepGroup) AddParent(parent *StepGroup) *StepGroup {
	s.Parents = append(s.Parents, parent)
	return s
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
		steps = append(steps, fmt.Sprintf("%d:%d", task.Step.Id, task.Id))
	}
	return strings.Join(steps, "-")
}

func (t *TaskGroup) RequiredResources() *pb.ComputeResource {

	resource := &pb.ComputeResource{
		CpuCount: 1,
		CpuLevel: 1,
	}

	for _, task := range t.Tasks {
		inst := task.Step.Instruction
		if inst != nil && task.Step.OutputDataset != nil {
			taskMemSize := inst.GetMemoryCostInMB(task.Step.OutputDataset.GetPartitionSize())
			resource.MemoryMb += taskMemSize
			// log.Printf("  %s : %s (%d MB)\n", t.String(), task.Step.Name, taskMemSize)
		}
	}

	return resource
}

func (t *TaskGroup) MarkStop(err error) {
	t.StopAt = time.Now()
	t.Error = err
	t.ParentStepGroup.waitForAllTasks.Broadcast()
}

func (s *StepGroup) WaitForAllTasksToComplete() {
	s.Lock()
	defer s.Unlock()

	for _, taskGroup := range s.TaskGroups {
		for taskGroup.StopAt.IsZero() || taskGroup.Error != nil {
			s.waitForAllTasks.Wait()
		}
	}
}
