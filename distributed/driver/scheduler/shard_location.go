package scheduler

import (
	"bytes"
	"sync"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
)

type DatasetShardLocator struct {
	sync.Mutex
	datasetShard2Location     map[string]pb.DataLocation
	datasetShard2LocationLock sync.Mutex
	waitForAllInputs          *sync.Cond
}

func NewDatasetShardLocator() *DatasetShardLocator {
	l := &DatasetShardLocator{
		datasetShard2Location: make(map[string]pb.DataLocation),
	}
	l.waitForAllInputs = sync.NewCond(l)
	return l
}

func (l *DatasetShardLocator) GetShardLocation(shardName string) (pb.DataLocation, bool) {
	l.datasetShard2LocationLock.Lock()
	defer l.datasetShard2LocationLock.Unlock()

	loc, hasValue := l.datasetShard2Location[shardName]
	return loc, hasValue
}

func (l *DatasetShardLocator) SetShardLocation(name string, location pb.DataLocation) {
	l.Lock()
	defer l.Unlock()

	l.datasetShard2LocationLock.Lock()
	defer l.datasetShard2LocationLock.Unlock()
	// fmt.Printf("shard %s is at %s\n", name, location.URL())
	l.datasetShard2Location[name] = location
	l.waitForAllInputs.Broadcast()
}

func (l *DatasetShardLocator) isDatasetShardRegistered(shard *flow.DatasetShard) bool {

	if _, hasValue := l.GetShardLocation(shard.Name()); !hasValue {
		// fmt.Printf("%s's waiting for %s, but it is not ready\n", shard.Dataset.Step.Name, shard.Name())
		return false
	}
	// fmt.Printf("%s knows %s is ready\n", shard.Dataset.Step.Name, shard.Name())
	return true
}

func (l *DatasetShardLocator) waitForInputDatasetShardLocations(task *flow.Task) {
	l.Lock()
	defer l.Unlock()

	for _, input := range task.InputShards {
		for !l.isDatasetShardRegistered(input) {
			l.waitForAllInputs.Wait()
		}
	}
}

func (l *DatasetShardLocator) waitForOutputDatasetShardLocations(task *flow.Task) {
	l.Lock()
	defer l.Unlock()

	for _, output := range task.OutputShards {
		for !l.isDatasetShardRegistered(output) {
			l.waitForAllInputs.Wait()
		}
	}
}

func (l *DatasetShardLocator) allInputLocations(task *flow.Task) string {
	l.Lock()
	defer l.Unlock()

	var buf bytes.Buffer
	for i, input := range task.InputShards {
		name := input.Name()
		location, hasValue := l.GetShardLocation(name)
		if !hasValue {
			panic("hmmm, we just checked all inputs are registered!")
		}
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(name)
		buf.WriteString("@")
		buf.WriteString(location.Location.URL())
	}
	return buf.String()
}
