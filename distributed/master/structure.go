package master

import (
	"sync"
	"time"

	"github.com/chrislusf/gleam/pb"
)

type AgentInformation struct {
	Location      pb.Location
	LastHeartBeat time.Time
	Resource      pb.ComputeResource
	Allocated     pb.ComputeResource
}

type Rack struct {
	sync.RWMutex
	Name      string
	Agents    map[string]*AgentInformation
	Resource  pb.ComputeResource
	Allocated pb.ComputeResource
}

type DataCenter struct {
	sync.RWMutex
	Name      string
	Racks     map[string]*Rack
	Resource  pb.ComputeResource
	Allocated pb.ComputeResource
}

type Topology struct {
	Resource  pb.ComputeResource
	Allocated pb.ComputeResource
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

func (dc *DataCenter) GetRacks() (ret []*Rack) {
	dc.RLock()
	defer dc.RUnlock()

	for _, v := range dc.Racks {
		r := v
		ret = append(ret, r)
	}
	return
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

func (rack *Rack) DropAgent(location *pb.Location) {
	rack.Lock()
	defer rack.Unlock()

	delete(rack.Agents, location.URL())
}

func (rack *Rack) GetAgents() (ret []*AgentInformation) {
	rack.RLock()
	defer rack.RUnlock()

	for _, v := range rack.Agents {
		a := v
		ret = append(ret, a)
	}
	return
}
