package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

// ExecuteTaskGroup wait for inputs and execute the task group remotely.
// If cancelled, the output will be cleaned up.
func (s *Scheduler) ExecuteTaskGroup(ctx context.Context, fc *flow.FlowContext, wg *sync.WaitGroup, taskGroup *plan.TaskGroup, bid float64) {

	defer wg.Done()

	tasks := taskGroup.Tasks
	lastTask := tasks[len(tasks)-1]
	if tasks[0].Step.IsOnDriverSide {
		// these should be only one task on the driver side
		s.localExecute(ctx, fc, lastTask, wg)
	} else {
		if !needsInputFromDriver(tasks[0]) {
			// wait until inputs are registed
			s.shardLocator.waitForInputDatasetShardLocations(tasks[0])
		}
		if isInputOnDisk(tasks[0]) && !isRestartableTasks(tasks) {
			// for non-restartable taskGroup, wait until on disk inputs are completed
			for _, stepGroup := range taskGroup.ParentStepGroup.Parents {
				stepGroup.WaitForAllTasksToComplete()
			}
		}

		// fmt.Printf("inputs of %s is %s\n", tasks[0].Name(), s.allInputLocations(tasks[0]))

		pickedServerChan := make(chan market.Supply, 1)
		s.Market.AddDemand(market.Requirement(taskGroup), bid, pickedServerChan)

		// get assigned executor location
		supply := <-pickedServerChan
		allocation := supply.Object.(*pb.Allocation)
		defer s.Market.ReturnSupply(supply)

		if needsInputFromDriver(tasks[0]) {
			// tell the driver to write to me
			for _, shard := range tasks[0].InputShards {
				// println("registering", shard.Name(), "at", allocation.Location.URL())
				s.setShardLocation(shard, pb.DataLocation{
					Name:     shard.Name(),
					Location: allocation.Location,
					OnDisk:   shard.Dataset.GetIsOnDiskIO(),
				})
			}
		}

		for _, shard := range lastTask.OutputShards {
			// println("registering", shard.Name(), "at", allocation.Location.URL(), "onDisk", shard.Dataset.GetIsOnDiskIO())
			s.setShardLocation(shard, pb.DataLocation{
				Name:     shard.Name(),
				Location: allocation.Location,
				OnDisk:   shard.Dataset.GetIsOnDiskIO(),
			})
		}

		fn := func() error {
			err := s.remoteExecuteOnLocation(ctx, fc, taskGroup, allocation, wg)
			taskGroup.MarkStop(err)
			return err
		}

		util.ExecuteOrCancel(
			ctx,
			func() error {
				if isRestartableTasks(tasks) {
					return util.TimeDelayedRetry(fn, time.Minute, 3*time.Minute)
				} else {
					return fn()
				}
			},
			func() {
				var w sync.WaitGroup
				for _, shard := range lastTask.OutputShards {
					w.Add(1)
					// println("deleting", shard.Name(), "from", allocation.Location.URL())
					go func(shard *flow.DatasetShard) {
						defer w.Done()
						if err := sendDeleteRequest(allocation.Location.URL(), &pb.DeleteDatasetShardRequest{
							Name: shard.Name(),
						}); err != nil {
							println("Purging dataset error:", err.Error())
						}
					}(shard)
				}
				w.Wait()
			},
		)
	}
}
