package distributed

import (
	"path/filepath"

	"github.com/chrislusf/gleam/distributed/driver"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
)

type DistributedOption struct {
	RequiredFiles []resource.FileResource
	Master        string
	DataCenter    string
	Rack          string
	TaskMemoryMB  int
	FlowBid       float64
	Module        string
	IsProfiling   bool
}

func Option() *DistributedOption {
	return &DistributedOption{
		Master:       "localhost:45326",
		DataCenter:   "",
		TaskMemoryMB: 64,
		FlowBid:      100.0,
	}
}

func (o *DistributedOption) GetFlowRunner() flow.FlowRunner {
	return driver.NewFlowDriver(&driver.Option{
		RequiredFiles: o.RequiredFiles,
		Master:        o.Master,
		DataCenter:    o.DataCenter,
		Rack:          o.Rack,
		TaskMemoryMB:  o.TaskMemoryMB,
		FlowBid:       o.FlowBid,
		Module:        o.Module,
		IsProfiling:   o.IsProfiling,
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

func (o *DistributedOption) SetProfiling(isProfiling bool) *DistributedOption {
	o.IsProfiling = isProfiling
	return o
}

// WithFile sends any related file over to gleam agents
// so the task can still access these files on gleam agents.
// The files are placed on the executed task's current working directory.
func (o *DistributedOption) WithFile(relatedFile, toFolder string) *DistributedOption {
	relativePath, err := filepath.Rel(".", relatedFile)
	if err != nil {
		relativePath = relatedFile
	}
	o.RequiredFiles = append(o.RequiredFiles, resource.FileResource{relativePath, toFolder})
	return o
}
