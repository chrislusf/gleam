package flow

func (fc *FlowContext) NewStep() (step *Step) {
	step = &Step{}
	fc.Steps = append(fc.Steps, step)
	return
}
