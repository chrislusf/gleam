package master

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/chrislusf/gleam/pb"
)

func (tp *Topology) allocateDataCenter(requests []*pb.ComputeResource) (string, error) {

	var total pb.ComputeResource
	for _, r := range requests {
		total = r.Plus(total)
	}
	dataCenters := tp.GetDataCenters()
	for name, dc := range dataCenters {
		if dc.Resource.Minus(dc.Allocated).Covers(total) {
			return name, nil
		}
	}

	if len(dataCenters) == 0 {
		return "", fmt.Errorf("No data center available.")
	}

	return "", fmt.Errorf("All data centers are busy.")
}

func (tp *Topology) allocateServersOnRack(dc *DataCenter, rack *Rack, requests []*pb.ComputeResource) (
	allocated []*pb.Allocation, remainingRequests []*pb.ComputeResource) {

	agents := rack.GetAgents()
	start := rand.Intn(len(agents)) - 1
	for _, req := range requests {
		request := req

		hasAllocation := false
		for x := 0; x < len(agents); x++ {
			start++
			if start == len(agents) {
				start = 0
			}
			agent := agents[start]

			available := agent.Resource.Minus(agent.Allocated)

			// fmt.Printf("available %v, requested %v\n", available, request.GetMemoryMb())
			if available.Covers(*request) {
				allocated = append(allocated, &pb.Allocation{
					Location:  &agent.Location,
					Allocated: request,
				})
				agent.Allocated = agent.Allocated.Plus(*request)
				rack.Allocated = rack.Allocated.Plus(*request)
				dc.Allocated = dc.Allocated.Plus(*request)
				tp.Allocated = tp.Allocated.Plus(*request)
				available = available.Minus(*request)
				hasAllocation = true
				break
			}
		}

		if !hasAllocation {
			remainingRequests = append(remainingRequests, request)
		}

	}

	return
}

func (tp *Topology) findServers(dc *DataCenter, requests []*pb.ComputeResource) (ret []*pb.Allocation) {

	// sort racks by unallocated resources
	var racks []*Rack
	for _, rack := range dc.GetRacks() {
		racks = append(racks, rack)
	}
	sort.Sort(byAvailableResources(racks))

	sort.Sort(byRequestedResources(requests))

	for _, rack := range racks {
		allocated, requests := tp.allocateServersOnRack(dc, rack, requests)
		ret = append(ret, allocated...)
		if len(requests) == 0 {
			break
		}
	}
	return
}

type byAvailableResources []*Rack

func (s byAvailableResources) Len() int      { return len(s) }
func (s byAvailableResources) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byAvailableResources) Less(i, j int) bool {
	return s[i].Resource.Minus(s[i].Allocated).Covers(s[j].Resource.Minus(s[j].Allocated))
}

type byRequestedResources []*pb.ComputeResource

func (s byRequestedResources) Len() int      { return len(s) }
func (s byRequestedResources) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byRequestedResources) Less(i, j int) bool {
	return s[i].Covers(*s[j])
}
