package resource

import (
	"sync"
	"time"
)

type AgentInformation struct {
	Location      Location
	LastHeartBeat time.Time
	Resource      ComputeResource
	Allocated     ComputeResource
}

type Rack struct {
	sync.RWMutex
	Name      string
	Agents    map[string]*AgentInformation
	Resource  ComputeResource
	Allocated ComputeResource
}

type DataCenter struct {
	sync.RWMutex
	Name      string
	Racks     map[string]*Rack
	Resource  ComputeResource
	Allocated ComputeResource
}

type Topology struct {
	Resource  ComputeResource
	Allocated ComputeResource
	sync.RWMutex
	DataCenters map[string]*DataCenter
}

func NewTopology() *Topology {
	return &Topology{
		DataCenters: make(map[string]*DataCenter),
	}
}

func NewDataCenter(name string) *DataCenter {
	return &DataCenter{
		Name:  name,
		Racks: make(map[string]*Rack),
	}
}

func NewRack(name string) *Rack {
	return &Rack{
		Name:   name,
		Agents: make(map[string]*AgentInformation),
	}
}

func (tp *Topology) ContainsDataCenters() bool {
	tp.RLock()
	defer tp.RUnlock()
	return len(tp.DataCenters) == 0
}

func (tp *Topology) GetDataCenter(name string) (*DataCenter, bool) {
	tp.RLock()
	defer tp.RUnlock()

	dc, ok := tp.DataCenters[name]
	return dc, ok
}

func (dc *DataCenter) GetRack(name string) (*Rack, bool) {
	dc.RLock()
	defer dc.RUnlock()

	rack, ok := dc.Racks[name]
	return rack, ok
}

func (rack *Rack) GetAgent(name string) (*AgentInformation, bool) {
	rack.RLock()
	defer rack.RUnlock()

	agentInformation, ok := rack.Agents[name]
	return agentInformation, ok
}

func (tp *Topology) AddDataCenter(dc *DataCenter) {
	tp.Lock()
	defer tp.Unlock()

	tp.DataCenters[dc.Name] = dc
}

func (tp *Topology) GetDataCenters() map[string]*DataCenter {
	tp.RLock()
	defer tp.RUnlock()

	s := make(map[string]*DataCenter, len(tp.DataCenters))
	for k, v := range tp.DataCenters {
		s[k] = v
	}
	return s
}

func (dc *DataCenter) GetRacks() map[string]*Rack {
	dc.RLock()
	defer dc.RUnlock()

	s := make(map[string]*Rack, len(dc.Racks))
	for k, v := range dc.Racks {
		s[k] = v
	}
	return s
}

func (dc *DataCenter) AddRack(rack *Rack) {
	dc.Lock()
	defer dc.Unlock()

	dc.Racks[rack.Name] = rack
}

func (rack *Rack) AddAgent(a *AgentInformation) {
	rack.Lock()
	defer rack.Unlock()

	rack.Agents[a.Location.URL()] = a
}

func (rack *Rack) DropAgent(a *AgentInformation) {
	rack.Lock()
	defer rack.Unlock()

	delete(rack.Agents, a.Location.URL())
}

func (rack *Rack) GetAgents() map[string]*AgentInformation {
	rack.RLock()
	defer rack.RUnlock()

	s := make(map[string]*AgentInformation, len(rack.Agents))
	for k, v := range rack.Agents {
		s[k] = v
	}
	return s
}
