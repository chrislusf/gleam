package flow

func (fc *FlowContext) NewStep() (step *Step) {
	step = &Step{Id: len(fc.Steps)}
	fc.Steps = append(fc.Steps, step)
	return
}
