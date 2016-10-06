package gleam

import (
	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/flow"
)

type FlowType int

const (
	Local FlowType = iota
	Distributed
)

func New(flowType ...FlowType) (fc *flow.FlowContext) {
	if len(flowType) > 0 {
		switch flowType[0] {
		case Distributed:
			return flow.New().SetRunner(driver.Distributed)
		}
	}
	return flow.New()
}

func NewDistributed() (fc *flow.FlowContext) {
	return New(Distributed)
}
