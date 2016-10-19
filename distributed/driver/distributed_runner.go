package driver

import (
	"github.com/chrislusf/gleam/flow"
)

type DistributedDriver struct {
	DriverOption
}

type DistributedPlanner struct {
	DriverOption
}

var (
	Distributed *DistributedDriver
	Planner     *DistributedPlanner
)

func init() {
	Distributed = &DistributedDriver{
		DriverOption: DriverOption{
			Master:       "localhost:45326",
			TaskMemoryMB: 64,
			FlowBid:      100.0,
			Host:         "localhost",
			Port:         0,
		},
	}
	Planner = &DistributedPlanner{}
}

func (r *DistributedDriver) RunFlowContext(fc *flow.FlowContext) {
	d := NewFlowContextDriver(&r.DriverOption)
	d.Run(fc)
}

func (r *DistributedPlanner) RunFlowContext(fc *flow.FlowContext) {
	d := NewFlowContextPlanner()
	d.Run(fc)
}
