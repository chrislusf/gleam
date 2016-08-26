package flow

func (step *Step) NewTask() (task *Task) {
	task = &Task{Step: step}
	step.Tasks = append(step.Tasks, task)
	return
}
