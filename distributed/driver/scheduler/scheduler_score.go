package scheduler

import (
	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
)

func (s *Scheduler) Score(r market.Requirement, bid float64, obj market.Object) float64 {
	tg, loc := r.(*plan.TaskGroup), obj.(resource.Allocation).Location
	firstTask := tg.Tasks[0]
	cost := float64(1)
	for _, input := range firstTask.InputShards {
		dataLocation, found := s.GetShardLocation(input)
		if !found {
			// log.Printf("Strange1: %s not allocated yet.", input.Name())
			continue
		}
		cost += dataLocation.Distance(loc)
	}
	return float64(bid) / cost
}
