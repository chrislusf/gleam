package scheduler

import (
	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
)

func (s *Scheduler) Score(r market.Requirement, bid float64, obj market.Object) float64 {
	alloc := obj.(resource.Allocation)
	tg, loc := r.(*plan.TaskGroup), alloc.Location

	memCost := memoryCost(tg)
	if memCost > alloc.Allocated.MemoryMB {
		return -1
	}

	firstTask := tg.Tasks[0]
	cost := float64(alloc.Allocated.MemoryMB-memCost) * 10
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

func memoryCost(tg *plan.TaskGroup) (cost int64) {
	for _, t := range tg.Tasks {
		if t.Step.Instruction != nil {
			cost += int64(t.Step.Instruction.GetMemoryCostInMB())
		}
	}
	return
}
