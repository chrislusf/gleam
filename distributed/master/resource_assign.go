package master

import (
	"fmt"
	"sort"

	pb "github.com/chrislusf/gleam/idl/master_rpc"
)

func (t *Topology) allocateDataCenter(requests []*pb.ComputeResource) (string, error) {

	var total pb.ComputeResource
	for _, r := range requests {
		total = r.Plus(total)
	}
	dataCenters := t.GetDataCenters()
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

func (t *Topology) allocateServersOnRack(dc *DataCenter, rack *Rack, requests []*pb.ComputeResource) (
	allocated []*pb.Allocation, remainingRequests []*pb.ComputeResource) {
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
			if available.Covers(*request) {
				allocated = append(allocated, &pb.Allocation{
					Location:  &agent.Location,
					Allocated: request,
				})
				agent.Allocated = agent.Allocated.Plus(*request)
				rack.Allocated = rack.Allocated.Plus(*request)
				dc.Allocated = dc.Allocated.Plus(*request)
				t.Allocated = t.Allocated.Plus(*request)
				available = available.Minus(*request)
				hasAllocation = true
			} else {
				remainingRequests = append(remainingRequests, request)
			}
		}
	}
	return
}

func (t *Topology) findServers(dc *DataCenter, requests []*pb.ComputeResource) (ret []*pb.Allocation) {

	// sort racks by unallocated resources
	var racks []*Rack
	for _, rack := range dc.GetRacks() {
		racks = append(racks, rack)
	}
	sort.Sort(byAvailableResources(racks))

	sort.Sort(byRequestedResources(requests))

	for _, rack := range racks {
		allocated, requests := t.allocateServersOnRack(dc, rack, requests)
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
