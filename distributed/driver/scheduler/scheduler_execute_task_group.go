package scheduler

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/driver/scheduler/market"
	"github.com/chrislusf/gleam/distributed/plan"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

// ExecuteTaskGroup wait for inputs and execute the task group remotely.
// If cancelled, the output will be cleaned up.
func (s *Scheduler) ExecuteTaskGroup(ctx context.Context,
	fc *flow.Flow,
	taskGroupStatus *pb.FlowExecutionStatus_TaskGroup,
	wg *sync.WaitGroup,
	taskGroup *plan.TaskGroup,
	bid float64, relatedFiles []resource.FileResource) {

	defer wg.Done()

	tasks := taskGroup.Tasks
	lastTask := tasks[len(tasks)-1]
	if tasks[0].Step.IsOnDriverSide {
		// these should be only one task on the driver side
		if err := taskGroupStatus.Track(func(exeStatus *pb.FlowExecutionStatus_TaskGroup_Execution) error {
			return s.localExecute(ctx, fc, exeStatus, lastTask, wg)
		}); err != nil {
			log.Fatalf("Failed to execute on driver side: %v", err)
		}
		return
	}
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

	// send driver code only when using go mapper reducer
	var hasGoCode bool
	for _, t := range tasks {
		hasGoCode = hasGoCode || t.Step.IsGoCode
	}
	if hasGoCode {
		relatedFiles = append(relatedFiles, resource.FileResource{os.Args[0], "."})
	}

	if len(relatedFiles) > 0 {
		err := withClient(allocation.Location.URL(), func(client pb.GleamAgentClient) error {
			for _, relatedFile := range relatedFiles {
				err := sendRelatedFile(ctx, client, fc.HashCode, relatedFile)
				if err != nil {
					taskGroup.MarkStop(err)
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Fatalf("Failed to send related files: %v", err)
		}
	}

	fn := func() error {
		err := taskGroupStatus.Track(func(exeStatus *pb.FlowExecutionStatus_TaskGroup_Execution) error {
			return s.remoteExecuteOnLocation(ctx, fc, taskGroupStatus, exeStatus, taskGroup, allocation, wg)
		})
		if err != nil {
			log.Printf("Failed to remoteExecuteOnLocation %v: %v", allocation, err)
		}
		taskGroup.MarkStop(err)
		return err
	}

	util.ExecuteWithCleanup(
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
