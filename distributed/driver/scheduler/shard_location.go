package scheduler

import (
	"bytes"
	"sync"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/flow"
)

type DatasetShardLocator struct {
	sync.Mutex
	executableFileHash        uint32
	datasetShard2Location     map[string]resource.Location
	datasetShard2LocationLock sync.Mutex
	waitForAllInputs          *sync.Cond
}

func NewDatasetShardLocator(executableFileHash uint32) *DatasetShardLocator {
	l := &DatasetShardLocator{
		executableFileHash:    executableFileHash,
		datasetShard2Location: make(map[string]resource.Location),
	}
	l.waitForAllInputs = sync.NewCond(l)
	return l
}

func (l *DatasetShardLocator) GetShardLocation(shardName string) (resource.Location, bool) {
	l.datasetShard2LocationLock.Lock()
	defer l.datasetShard2LocationLock.Unlock()

	loc, hasValue := l.datasetShard2Location[shardName]
	return loc, hasValue
}

func (l *DatasetShardLocator) SetShardLocation(name string, location resource.Location) {
	l.Lock()
	defer l.Unlock()

	l.datasetShard2LocationLock.Lock()
	defer l.datasetShard2LocationLock.Unlock()
	// fmt.Printf("shard %s is at %s\n", name, location.URL())
	l.datasetShard2Location[name] = location
	l.waitForAllInputs.Broadcast()
}

func (l *DatasetShardLocator) allInputsAreRegistered(task *flow.Task) bool {

	for _, input := range task.InputShards {
		if _, hasValue := l.GetShardLocation(GetUniqueShardName(l.executableFileHash, input)); !hasValue {
			// fmt.Printf("%s's input %s is not ready\n", task.Name(), input.Name())
			return false
		}
	}
	return true
}

func (l *DatasetShardLocator) waitForInputDatasetShardLocations(task *flow.Task) {
	l.Lock()
	defer l.Unlock()

	for !l.allInputsAreRegistered(task) {
		l.waitForAllInputs.Wait()
	}
}

func (l *DatasetShardLocator) allInputLocations(task *flow.Task) string {
	l.Lock()
	defer l.Unlock()

	var buf bytes.Buffer
	for i, input := range task.InputShards {
		name := GetUniqueShardName(l.executableFileHash, input)
		location, hasValue := l.GetShardLocation(name)
		if !hasValue {
			panic("hmmm, we just checked all inputs are registered!")
		}
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(name)
		buf.WriteString("@")
		buf.WriteString(location.URL())
	}
	return buf.String()
}
