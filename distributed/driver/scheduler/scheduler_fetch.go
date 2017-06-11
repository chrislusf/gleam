package scheduler

import (
	"log"
	"math/rand"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/pb"
)

// Requirement is TaskGroup
// Object is Agent's Location
func (s *Scheduler) Fetch(demands []market.Demand) {
	var request pb.ComputeRequest
	request.Username = s.Option.Username
	request.Hostname = s.Option.Hostname
	request.FlowHashCode = s.Option.FlowHashcode
	request.DataCenter = s.Option.DataCenter
	for _, d := range demands {
		taskGroup := d.Requirement.(*plan.TaskGroup)
		requiredResource := taskGroup.RequiredResources()
		request.ComputeResources = append(request.ComputeResources, requiredResource)
	}

	result, err := getResources(s.Master, &request)
	if err != nil {
		log.Printf("%s Failed to allocate: %v", s.Master, err)
		time.Sleep(time.Millisecond * time.Duration(15000+rand.Int63n(5000)))
	} else {
		if len(result.Allocations) == 0 {
			// log.Printf("%s No more new executors.", s.Master)
			time.Sleep(time.Millisecond * time.Duration(2000+rand.Int63n(1000)))
		} else {
			if s.Option.DataCenter == "" {
				s.Option.DataCenter = result.Allocations[0].Location.DataCenter
			}
			var allocatedMemory int64
			for _, allocation := range result.Allocations {
				s.Market.AddSupply(market.Supply{
					Object: allocation,
				})
				allocatedMemory += allocation.Allocated.MemoryMb
			}
			// log.Printf("%s allocated %d executors with %d MB memory.", s.Master, len(result.Allocations), allocatedMemory)
		}
	}
}
