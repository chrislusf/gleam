package driver

import (
	"github.com/chrislusf/gleam/flow"
)

type DistributedDriver struct {
	DriverOption
}

var (
	Distributed *DistributedDriver
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
}

func (r *DistributedDriver) RunFlowContext(fc *flow.FlowContext) {
	d := NewFlowContextDriver(&r.DriverOption)
	d.Run(fc)
}
