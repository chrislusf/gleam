package distributed

import (
	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/flow"
)

type DistributedOption struct {
	Master       string
	DataCenter   string
	Rack         string
	TaskMemoryMB int
	FlowBid      float64
	Module       string
	Host         string
	Port         int
}

func Option() *DistributedOption {
	return &DistributedOption{
		Master:       "localhost:45326",
		DataCenter:   "",
		TaskMemoryMB: 64,
		FlowBid:      100.0,
		Host:         "localhost",
		Port:         0,
	}
}

func (o *DistributedOption) GetFlowRunner() flow.FlowRunner {
	return driver.NewFlowContextDriver(&driver.Option{
		Master:       o.Master,
		DataCenter:   o.DataCenter,
		Rack:         o.Rack,
		TaskMemoryMB: o.TaskMemoryMB,
		FlowBid:      o.FlowBid,
		Module:       o.Module,
		Host:         o.Host,
		Port:         o.Port,
	})
}

func (o *DistributedOption) SetDataCenter(dataCenter string) *DistributedOption {
	o.DataCenter = dataCenter
	return o
}

func (o *DistributedOption) SetMaster(master string) *DistributedOption {
	o.Master = master
	return o
}
