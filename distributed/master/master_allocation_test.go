package master

import (
	"fmt"
	"testing"

	"github.com/chrislusf/gleam/distributed/resource"
)

func TestAllocation1(t *testing.T) {

	lr := NewMasterResource()
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc1",
			Rack:       "rack1",
			Server:     "server1",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 1,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc1",
			Rack:       "rack2",
			Server:     "server2",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 1,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc2",
			Rack:       "rack3",
			Server:     "server3",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 16,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})

	req := &resource.AllocationRequest{
		Requests: []resource.ComputeRequest{
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 1024,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 1024,
				},
				Inputs: nil,
			},
		},
	}

	tl := &TeamMaster{}
	tl.MasterResource = lr

	result := tl.allocate(req)
	t.Logf("Result: %+v", result)

	fmt.Printf("===========Start test 2============\n")
	req2 := &resource.AllocationRequest{
		Requests: []resource.ComputeRequest{
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
		},
	}
	result2 := tl.allocate(req2)
	t.Logf("Result: %+v", result2)

}

func TestAllocation2(t *testing.T) {

	lr := NewMasterResource()
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc1",
			Rack:       "rack1",
			Server:     "server1",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 16,
			CPULevel: 1,
			MemoryMB: 1024,
		},
	})

	tl := &TeamMaster{}
	tl.MasterResource = lr

	req := &resource.AllocationRequest{
		Requests: []resource.ComputeRequest{
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 16,
				},
				Inputs: nil,
			},
		},
	}
	result := tl.allocate(req)
	t.Logf("Result2: %+v", result)

}

func TestAllocation3(t *testing.T) {

	lr := NewMasterResource()
	lr.UpdateAgentInformation(&resource.AgentInformation{
		Location: resource.Location{
			DataCenter: "dc1",
			Rack:       "rack1",
			Server:     "server1",
			Port:       1111,
		},
		Resource: resource.ComputeResource{
			CPUCount: 16,
			CPULevel: 4,
			MemoryMB: 2048,
		},
	})

	req := &resource.AllocationRequest{
		Requests: []resource.ComputeRequest{
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 1024,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 512,
				},
				Inputs: nil,
			},
			resource.ComputeRequest{
				ComputeResource: resource.ComputeResource{
					CPUCount: 1,
					CPULevel: 1,
					MemoryMB: 256,
				},
				Inputs: nil,
			},
		},
	}

	tl := &TeamMaster{}
	tl.MasterResource = lr

	result := tl.allocate(req)
	t.Logf("Result: %+v", result)

	// assert the results
	if result.Error != "" {
		t.Fatal(result.Error)
	}
	if len(result.Allocations) == 0 {
		t.Fatal("no resource was allocated")
	}
	if !isAllocated(result.Allocations, req.Requests[0].ComputeResource) {
		t.Error("resources of the first request were not allocated")
	}
	if !isAllocated(result.Allocations, req.Requests[1].ComputeResource) {
		t.Error("resources of the second request were not allocated")
	}
	if !isAllocated(result.Allocations, req.Requests[2].ComputeResource) {
		t.Error("resources of the third request were not allocated")
	}

}

func isAllocated(als []resource.Allocation, res resource.ComputeResource) bool {
	for _, al := range als {
		if al.Allocated == res {
			return true
		}
	}
	return false
}
