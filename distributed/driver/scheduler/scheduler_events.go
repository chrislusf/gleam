package scheduler

import (
	"sync"

	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
)

type TaskGroupStatus struct {
	FlowContext *flow.FlowContext
	TaskGroup   *plan.TaskGroup
	Completed   bool
	Error       error
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
		case ReleaseTaskGroupInputs:
			go func() {
				defer event.WaitGroup.Done()

				var wg sync.WaitGroup
				for _, taskGroup := range event.TaskGroups {
					tasks := taskGroup.Tasks
					for _, shard := range tasks[len(tasks)-1].OutputShards {
						location, _ := s.getShardLocation(shard)
						// println("deleting", shard.Name(), "on", location.URL())
						if location.Location == nil {
							continue
						}
						wg.Add(1)
						go func(location pb.DataLocation, shard *flow.DatasetShard) {
							defer wg.Done()
							if err := sendDeleteRequest(location.Location.URL(), &pb.DeleteDatasetShardRequest{
								Name: shard.Name(),
							}); err != nil {
								println("Purging dataset error:", err.Error())
							}
						}(location, shard)
					}
				}
				wg.Wait()

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

func isInputOnDisk(task *flow.Task) bool {
	for _, shard := range task.InputShards {
		if shard.Dataset.Meta.OnDisk != flow.ModeOnDisk {
			return false
		}
	}
	return true
}

func isRestartableTasks(tasks []*flow.Task) bool {
	for _, task := range tasks {
		if !task.Step.Meta.IsRestartable {
			return false
		}
	}
	return true
}

func (s *Scheduler) getShardLocation(shard *flow.DatasetShard) (pb.DataLocation, bool) {
	location, found := s.shardLocator.GetShardLocation(shard.Name())
	return location, found
}

func (s *Scheduler) setShardLocation(shard *flow.DatasetShard, loc pb.DataLocation) {
	s.shardLocator.SetShardLocation(shard.Name(), loc)
}
