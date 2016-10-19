package gleam

import (
	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/flow"
)

type FlowType int

const (
	Local FlowType = iota
	Distributed
	DistributedPlanner
)

func New(flowType ...FlowType) (fc *flow.FlowContext) {
	if len(flowType) > 0 {
		switch flowType[0] {
		case Distributed:
			return flow.New().SetRunner(driver.Distributed)
		case DistributedPlanner:
			return flow.New().SetRunner(driver.Planner)
		}
	}
	return flow.New()
}

func NewDistributed() (fc *flow.FlowContext) {
	return New(Distributed)
}

func SetMaster(fc *flow.FlowContext, master string) {
	if distributedRunner, ok := fc.Runner.(*driver.DistributedDriver); ok {
		distributedRunner.Master = master
	}
}
