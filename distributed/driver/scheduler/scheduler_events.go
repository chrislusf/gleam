package scheduler

import (
	"fmt"
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

				// wait until inputs are registed
				s.shardLocator.waitForInputDatasetShardLocations(tasks[0])
				// fmt.Printf("inputs of %s is %s\n", tasks[0].Name(), s.allInputLocations(tasks[0]))

				pickedServerChan := make(chan market.Supply, 1)
				s.Market.AddDemand(market.Requirement(taskGroup), event.Bid, pickedServerChan)

				// get assigned executor location
				supply := <-pickedServerChan
				allocation := supply.Object.(resource.Allocation)
				defer s.Market.ReturnSupply(supply)

				s.remoteExecuteOnLocation(event.FlowContext, taskGroup, allocation, event.WaitGroup)
			}()
		case ReleaseTaskGroupInputs:
			go func() {
				defer event.WaitGroup.Done()

				for _, taskGroup := range event.TaskGroups {
					tasks := taskGroup.Tasks
					for _, shard := range tasks[len(tasks)-1].OutputShards {
						shardName, location, _ := s.GetShardLocation(shard)
						request := NewDeleteDatasetShardRequest(shardName)
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

func (s *Scheduler) GetShardLocation(shard *flow.DatasetShard) (string, resource.Location, bool) {
	shardName := s.GetUniqueShardName(shard)
	location, found := s.shardLocator.GetShardLocation(shardName)
	return shardName, location, found
}

func (s *Scheduler) SetShardLocation(shard *flow.DatasetShard, loc resource.Location) {
	shardName := s.GetUniqueShardName(shard)
	s.shardLocator.SetShardLocation(shardName, loc)
}

func (s *Scheduler) GetUniqueShardName(shard *flow.DatasetShard) string {
	return GetUniqueShardName(s.Option.ExecutableFileHash, shard)
}

func GetUniqueShardName(executableFileHash uint32, shard *flow.DatasetShard) string {
	return fmt.Sprintf("%d-%s", executableFileHash, shard.Name())
}
