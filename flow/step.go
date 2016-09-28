package flow

func (fc *FlowContext) NewStep() (step *Step) {
	step = &Step{Id: len(fc.Steps)}
	fc.Steps = append(fc.Steps, step)
	return
}

func (step *Step) NewTask() (task *Task) {
	task = &Task{Step: step, Id: len(step.Tasks)}
	step.Tasks = append(step.Tasks, task)
	return
}
