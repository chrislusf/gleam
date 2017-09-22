package scheduler

import (
	"sync"

	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
)

func (s *Scheduler) DeleteOutput(taskGroup *plan.TaskGroup) {
	var wg sync.WaitGroup
	tasks := taskGroup.Tasks
	for _, shard := range tasks[len(tasks)-1].OutputShards {
		location, _ := s.GetShardLocation(shard)
		if location.Location == nil {
			continue
		}
		// println("deleting", shard.Name(), "on", location.GetLocation().URL())
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
	wg.Wait()
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

func (s *Scheduler) GetShardLocation(shard *flow.DatasetShard) (pb.DataLocation, bool) {
	location, found := s.shardLocator.GetShardLocation(shard.Name())
	return location, found
}

func (s *Scheduler) setShardLocation(shard *flow.DatasetShard, loc pb.DataLocation) {
	s.shardLocator.SetShardLocation(shard.Name(), loc)
}
