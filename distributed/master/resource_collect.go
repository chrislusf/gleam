package master

import (
	"time"

	"github.com/chrislusf/gleam/pb"
)

func (l *Topology) UpdateAgentInformation(ai *pb.Heartbeat) {
	dc, hasDc := l.GetDataCenter(ai.Location.DataCenter)
	if !hasDc {
		dc = NewDataCenter(ai.Location.DataCenter)
		l.AddDataCenter(dc)
	}

	rack, hasRack := dc.GetRack(ai.Location.Rack)
	if !hasRack {
		rack = NewRack(ai.Location.Rack)
		dc.AddRack(rack)
	}

	oldInfo, hasOldInfo := rack.GetAgent(ai.Location.URL())
	deltaResource := *ai.Resource
	// fmt.Printf("hasOldInfo %+v, oldInfo %+v\n", hasOldInfo, oldInfo)
	if hasOldInfo {
		deltaResource = deltaResource.Minus(oldInfo.Resource)
		if !deltaResource.IsZero() {
			oldInfo.Resource = *ai.Resource
		}
		oldInfo.LastHeartBeat = time.Now()
	} else {
		rack.AddAgent(&AgentInformation{
			Location:      *ai.Location,
			LastHeartBeat: time.Now(),
			Resource:      *ai.Resource,
			Allocated:     *ai.Allocated,
		})
	}

	l.Lock()
	defer l.Unlock()

	if !deltaResource.IsZero() {
		rack.Resource = rack.Resource.Plus(deltaResource)
		dc.Resource = dc.Resource.Plus(deltaResource)
		l.Resource = l.Resource.Plus(deltaResource)
	}

	if hasOldInfo {
		deltaAllocated := ai.Allocated.Minus(oldInfo.Allocated)
		oldInfo.Allocated = *ai.Allocated
		// fmt.Printf("deltaAllocated %+v\n", deltaAllocated)
		if !deltaAllocated.IsZero() {
			rack.Allocated = rack.Allocated.Plus(deltaAllocated)
			dc.Allocated = dc.Allocated.Plus(deltaAllocated)
			l.Allocated = l.Allocated.Plus(deltaAllocated)
		}
	}

}

func (l *Topology) deleteAgentInformation(location *pb.Location) {

	dc, hasDc := l.GetDataCenter(location.DataCenter)
	if !hasDc {
		return
	}

	rack, hasRack := dc.GetRack(location.Rack)
	if !hasRack {
		return
	}

	oldInfo, hasOldInfo := rack.GetAgent(location.URL())
	if !hasOldInfo {
		return
	}

	l.Lock()
	defer l.Unlock()

	deltaResource := oldInfo.Resource
	deltaAllocated := oldInfo.Allocated

	if !deltaResource.IsZero() {
		// fmt.Printf("deleting %+v\n", oldInfo)
		rack.DropAgent(location)

		rack.Resource = rack.Resource.Minus(deltaResource)
		rack.Allocated = rack.Allocated.Minus(deltaAllocated)
		dc.Resource = dc.Resource.Minus(deltaResource)
		dc.Allocated = dc.Allocated.Minus(deltaAllocated)
		l.Resource = l.Resource.Minus(deltaResource)
		l.Allocated = l.Allocated.Minus(deltaAllocated)
	}

}

func (l *Topology) findAgentInformation(location *pb.Location) (*AgentInformation, bool) {
	d, hasDc := l.GetDataCenter(location.DataCenter)
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
