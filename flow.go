package gleam

import (
	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/flow"
)

func NewFlow() (fc *flow.FlowContext) {
	return flow.New()
}

func NewDistributedFlow() (fc *flow.FlowContext) {
	return flow.New().SetRunner(driver.Distributed)
}
