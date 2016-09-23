package master

import (
	// "fmt"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/util"
)

const TimeOutLimit = 15 // seconds

type ResourceUpdateEvent struct {
	DataCenter string
	Rack       string
}

type MasterResource struct {
	Topology      resource.Topology
	EventChan     chan interface{}
	EvictionQueue *util.PriorityQueue
	sync.Mutex
}

func NewMasterResource() *MasterResource {
	l := &MasterResource{
		Topology:  *resource.NewTopology(),
		EventChan: make(chan interface{}, 1),
		EvictionQueue: util.NewPriorityQueue(func(a, b interface{}) bool {
			x, y := a.(*resource.AgentInformation), b.(*resource.AgentInformation)
			return x.LastHeartBeat.Before(y.LastHeartBeat)
		}),
	}

	go l.BackgroundEventLoop()
	go l.BackgroundEvictionLoop()

	return l
}

func (l *MasterResource) UpdateAgentInformation(ai *resource.AgentInformation) {
	l.Lock()
	defer l.Unlock()

	dc, hasDc := l.Topology.GetDataCenter(ai.Location.DataCenter)
	if !hasDc {
		dc = resource.NewDataCenter(ai.Location.DataCenter)
		l.Topology.AddDataCenter(dc)
	}

	rack, hasRack := dc.GetRack(ai.Location.Rack)
	if !hasRack {
		rack = resource.NewRack(ai.Location.Rack)
		dc.AddRack(rack)
	}

	oldInfo, hasOldInfo := rack.GetAgent(ai.Location.URL())
	deltaResource := ai.Resource
	// fmt.Printf("hasOldInfo %+v, oldInfo %+v\n", hasOldInfo, oldInfo)
	if hasOldInfo {
		deltaResource = deltaResource.Minus(oldInfo.Resource)
		if !deltaResource.IsZero() {
			oldInfo.Resource = ai.Resource
		}
		oldInfo.LastHeartBeat = time.Now()
		l.EvictionQueue.Enqueue(ai, 0)
	} else {
		rack.AddAgent(ai)
		ai.LastHeartBeat = time.Now()
		l.EvictionQueue.Enqueue(ai, 0)
	}

	if !deltaResource.IsZero() {
		// fmt.Printf("updating %+v\n", deltaResource)
		l.EventChan <- ResourceUpdateEvent{ai.Location.DataCenter, ai.Location.Rack}

		rack.Resource = rack.Resource.Plus(deltaResource)
		dc.Resource = dc.Resource.Plus(deltaResource)
		l.Topology.Resource = l.Topology.Resource.Plus(deltaResource)
	}

	if hasOldInfo {
		deltaAllocated := ai.Allocated.Minus(oldInfo.Allocated)
		oldInfo.Allocated = ai.Allocated
		// fmt.Printf("deltaAllocated %+v\n", deltaAllocated)
		if !deltaAllocated.IsZero() {
			rack.Allocated = rack.Allocated.Plus(deltaAllocated)
			dc.Allocated = dc.Allocated.Plus(deltaAllocated)
			l.Topology.Allocated = l.Topology.Allocated.Plus(deltaAllocated)
		}
	}

}

func (l *MasterResource) deleteAgentInformation(ai *resource.AgentInformation) {
	l.Lock()
	defer l.Unlock()

	dc, hasDc := l.Topology.GetDataCenter(ai.Location.DataCenter)
	if !hasDc {
		return
	}

	rack, hasRack := dc.GetRack(ai.Location.Rack)
	if !hasRack {
		return
	}

	oldInfo, hasOldInfo := rack.GetAgent(ai.Location.URL())
	if !hasOldInfo {
		return
	}

	deltaResource := oldInfo.Resource
	deltaAllocated := oldInfo.Allocated

	if !deltaResource.IsZero() {
		// fmt.Printf("deleting %+v\n", oldInfo)
		rack.DropAgent(ai)
		l.EventChan <- ResourceUpdateEvent{ai.Location.DataCenter, ai.Location.Rack}

		rack.Resource = rack.Resource.Minus(deltaResource)
		rack.Allocated = rack.Allocated.Minus(deltaAllocated)
		dc.Resource = dc.Resource.Minus(deltaResource)
		dc.Allocated = dc.Allocated.Minus(deltaAllocated)
		l.Topology.Resource = l.Topology.Resource.Minus(deltaResource)
		l.Topology.Allocated = l.Topology.Allocated.Minus(deltaAllocated)
	}

}

func (l *MasterResource) findAgentInformation(location resource.Location) (*resource.AgentInformation, bool) {
	d, hasDc := l.Topology.GetDataCenter(location.DataCenter)
	if !hasDc {
		return nil, false
	}

	r, hasRack := d.GetRack(location.Rack)
	if !hasRack {
		return nil, false
	}

	ai, ok := r.GetAgent(location.URL())
	return ai, ok
}

func (l *MasterResource) BackgroundEvictionLoop() {
	for {
		if l.EvictionQueue.Len() == 0 {
			// println("eviction: sleep for", TimeOutLimit, "seconds")
			time.Sleep(TimeOutLimit * time.Second)
			continue
		}
		obj, _ := l.EvictionQueue.Dequeue()
		a := obj.(*resource.AgentInformation)
		ai, found := l.findAgentInformation(a.Location)
		if !found {
			continue
		}
		if ai.LastHeartBeat.Add(TimeOutLimit * time.Second).After(time.Now()) {
			// println("eviction 1: sleep for", ai.LastHeartBeat.Add(TimeOutLimit*time.Second).Sub(time.Now()))
			time.Sleep(ai.LastHeartBeat.Add(TimeOutLimit * time.Second).Sub(time.Now()))
		}
		ai, found = l.findAgentInformation(ai.Location)
		if !found {
			continue
		}
		if ai.LastHeartBeat.Add(TimeOutLimit * time.Second).Before(time.Now()) {
			// this has not been refreshed since last heart beat
			l.deleteAgentInformation(ai)
		}
	}
}

func (l *MasterResource) BackgroundEventLoop() {
	for {
		event := <-l.EventChan
		switch event.(type) {
		default:
		case ResourceUpdateEvent:
			// fmt.Printf("update %s:%s\n", event.DataCenter, event.Rack)
		}
	}
}
