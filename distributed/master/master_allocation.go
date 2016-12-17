package master

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/chrislusf/gleam/distributed/resource"
)

func (tl *TeamMaster) allocate(req *resource.AllocationRequest) (result *resource.AllocationResult) {
	result = &resource.AllocationResult{}
	dc, err := tl.findDataCenter(req)
	if err != nil {
		result.Error = err.Error()
		return
	}

	allocations := tl.findServers(dc, req)

	result.Allocations = allocations
	return
}

func (tl *TeamMaster) allocateServersOnRack(dc *resource.DataCenter, rack *resource.Rack, requests []*resource.ComputeRequest) (
	allocated []resource.Allocation, remainingRequests []*resource.ComputeRequest) {
	var j = -1
	for _, agent := range rack.GetAgents() {
		if j >= len(requests) {
			break
		}
		available := agent.Resource.Minus(agent.Allocated)
		hasAllocation := true
		for available.GreaterThanZero() && hasAllocation && j < len(requests) {
			hasAllocation = false

			j++
			if j >= len(requests) {
				break
			}
			request := requests[j]

			// fmt.Printf("available %v, requested %v\n", available, request.ComputeResource)
			if available.Covers(request.ComputeResource) {
				allocated = append(allocated, resource.Allocation{
					Location:  agent.Location,
					Allocated: request.ComputeResource,
				})
				agent.Allocated = agent.Allocated.Plus(request.ComputeResource)
				rack.Allocated = rack.Allocated.Plus(request.ComputeResource)
				dc.Allocated = dc.Allocated.Plus(request.ComputeResource)
				tl.MasterResource.Topology.Allocated = tl.MasterResource.Topology.Allocated.Plus(request.ComputeResource)
				available = available.Minus(request.ComputeResource)
				hasAllocation = true
			} else {
				remainingRequests = append(remainingRequests, request)
			}
		}
	}
	return
}

func (tl *TeamMaster) findServers(dc *resource.DataCenter, req *resource.AllocationRequest) (ret []resource.Allocation) {
	// sort racks by unallocated resources
	var racks []*resource.Rack
	for _, rack := range dc.GetRacks() {
		racks = append(racks, rack)
	}
	sort.Sort(ByAvailableResources(racks))

	requests := make([]*resource.ComputeRequest, len(req.Requests))
	for i := range req.Requests {
		requests[i] = &req.Requests[i]
	}
	sort.Sort(ByRequestedResources(requests))

	for _, rack := range racks {
		allocated, requests := tl.allocateServersOnRack(dc, rack, requests)
		ret = append(ret, allocated...)
		if len(requests) == 0 {
			break
		}
	}
	return
}

func (tl *TeamMaster) findDataCenter(req *resource.AllocationRequest) (*resource.DataCenter, error) {
	// calculate total resource requested
	var totalComputeResource resource.ComputeResource
	for _, cr := range req.Requests {
		totalComputeResource = totalComputeResource.Plus(cr.ComputeResource)
	}

	// check preferred data center
	// TODO: assign for each data center, instead of just the last data center
	dcName := ""
	for _, cr := range req.Requests {
		for _, input := range cr.Inputs {
			dcName = input.Location.DataCenter
		}
	}
	if dcName != "" {
		dc, hasDc := tl.MasterResource.Topology.GetDataCenter(dcName)
		if !hasDc {
			return nil, fmt.Errorf("Failed to find existing data center: %s", dcName)
		}
		return dc, nil
	}

	if !tl.MasterResource.Topology.ContainsDataCenters() {
		return nil, fmt.Errorf("No data centers found.")
	}

	// weighted reservior sampling
	var selectedDc *resource.DataCenter
	var seenWeight int64
	for _, dc := range tl.MasterResource.Topology.GetDataCenters() {
		available := dc.Resource.Minus(dc.Allocated)
		weight := available.MemoryMB
		if weight > 0 {
			seenWeight += weight
			if rand.Int63n(seenWeight) < weight {
				selectedDc = dc
			}
		}
	}
	if seenWeight == 0 {
		return nil, fmt.Errorf("No data center is free.")
	}

	return selectedDc, nil
}

type ByAvailableResources []*resource.Rack

func (s ByAvailableResources) Len() int      { return len(s) }
func (s ByAvailableResources) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByAvailableResources) Less(i, j int) bool {
	return s[i].Resource.Minus(s[i].Allocated).Covers(s[j].Resource.Minus(s[j].Allocated))
}

type ByRequestedResources []*resource.ComputeRequest

func (s ByRequestedResources) Len() int      { return len(s) }
func (s ByRequestedResources) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByRequestedResources) Less(i, j int) bool {
	return s[i].ComputeResource.Covers(s[j].ComputeResource)
}
