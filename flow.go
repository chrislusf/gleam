package gleam

import (
	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/flow"
)

func New() (fc *flow.FlowContext) {
	return flow.New()
}

func NewDistributed() (fc *flow.FlowContext) {
	return flow.New().SetRunner(driver.Distributed)
}
