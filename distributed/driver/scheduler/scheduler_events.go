package scheduler

import (
	"sync"

	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
)

type SubmitTaskGroup struct {
	FlowContext *flow.FlowContext
	TaskGroup   *plan.TaskGroup
	Bid         float64
	WaitGroup   *sync.WaitGroup
}

type ReleaseTaskGroupInputs struct {
	FlowContext *flow.FlowContext
	TaskGroups  []*plan.TaskGroup
	WaitGroup   *sync.WaitGroup
}

/*
resources are leased to driver, expires every X miniute unless renewed.
1. request resource
2. release resource
*/
func (s *Scheduler) EventLoop() {
	for {
		event := <-s.EventChan
		switch event := event.(type) {
		default:
		case SubmitTaskGroup:
			// fmt.Printf("processing %+v\n", event)
			taskGroup := event.TaskGroup
			go func() {
				defer event.WaitGroup.Done()
				tasks := event.TaskGroup.Tasks
				if tasks[0].Step.IsOnDriverSide {
					// these should be only one task on the driver side
					lastTask := tasks[len(tasks)-1]
					s.localExecute(event.FlowContext, lastTask, event.WaitGroup)
				} else {
					if !needsInputFromDriver(tasks[0]) {
						// wait until inputs are registed
						s.shardLocator.waitForInputDatasetShardLocations(tasks[0])
					}

					// fmt.Printf("inputs of %s is %s\n", tasks[0].Name(), s.allInputLocations(tasks[0]))

					pickedServerChan := make(chan market.Supply, 1)
					s.Market.AddDemand(market.Requirement(taskGroup), event.Bid, pickedServerChan)

					// get assigned executor location
					supply := <-pickedServerChan
					allocation := supply.Object.(resource.Allocation)
					defer s.Market.ReturnSupply(supply)

					if needsInputFromDriver(tasks[0]) {
						// tell the driver to write to me
						for _, shard := range tasks[0].InputShards {
							// println("registering", shard.Name(), "at", allocation.Location.URL())
							s.SetShardLocation(shard, allocation.Location)
						}
					}

					for _, shard := range tasks[len(tasks)-1].OutputShards {
						// println("registering", shard.Name(), "at", allocation.Location.URL())
						s.SetShardLocation(shard, allocation.Location)
					}
					// println("sending task group started with:", tasks[0].Step.Name)
					s.remoteExecuteOnLocation(event.FlowContext, taskGroup, allocation, event.WaitGroup)
				}
			}()
		case ReleaseTaskGroupInputs:
			go func() {
				defer event.WaitGroup.Done()

				for _, taskGroup := range event.TaskGroups {
					tasks := taskGroup.Tasks
					for _, shard := range tasks[len(tasks)-1].OutputShards {
						location, _ := s.GetShardLocation(shard)
						request := NewDeleteDatasetShardRequest(shard.Name())
						// println("deleting", ds.Name(), "on", location.URL())
						if err := RemoteDirectExecute(location.URL(), request); err != nil {
							println("Purging dataset error:", err.Error())
						}
					}
				}

			}()
		}
	}
}

func needsInputFromDriver(task *flow.Task) bool {
	for _, shard := range task.InputShards {
		if shard.Dataset.Step.IsOnDriverSide {
			return true
		}
	}
	return false
}

func (s *Scheduler) GetShardLocation(shard *flow.DatasetShard) (resource.Location, bool) {
	location, found := s.shardLocator.GetShardLocation(shard.Name())
	return location, found
}

func (s *Scheduler) SetShardLocation(shard *flow.DatasetShard, loc resource.Location) {
	s.shardLocator.SetShardLocation(shard.Name(), loc)
}
