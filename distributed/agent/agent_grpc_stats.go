package agent

import (
	"fmt"
	"sync"

	"github.com/chrislusf/gleam/pb"
)

var (
	statsChanMap        = make(map[string]chan *pb.ExecutionStat)
	statsChanMapRWMutex sync.Mutex
)

func getStatsChan(flowHashCode uint32, stepId int32, taskId int32) chan *pb.ExecutionStat {
	key := fmt.Sprintf("%d-%d-%d", flowHashCode, stepId, taskId)
	statsChanMapRWMutex.Lock()
	defer statsChanMapRWMutex.Unlock()
	return statsChanMap[key]
}

func deleteStatsChanByInstructionSet(instructionSet *pb.InstructionSet) {
	deleteStatsChan(instructionSet.FlowHashCode,
		instructionSet.Instructions[0].GetStepId(),
		instructionSet.Instructions[0].GetTaskId())
}

func deleteStatsChan(flowHashCode uint32, stepId int32, taskId int32) {
	key := fmt.Sprintf("%d-%d-%d", flowHashCode, stepId, taskId)
	statsChanMapRWMutex.Lock()
	defer statsChanMapRWMutex.Unlock()
	delete(statsChanMap, key)
}

func createStatsChanByInstructionSet(instructionSet *pb.InstructionSet) chan *pb.ExecutionStat {
	statsChan := make(chan *pb.ExecutionStat)
	key := fmt.Sprintf("%d-%d-%d",
		instructionSet.FlowHashCode,
		instructionSet.Instructions[0].GetStepId(),
		instructionSet.Instructions[0].GetTaskId())
	statsChanMapRWMutex.Lock()
	defer statsChanMapRWMutex.Unlock()
	statsChanMap[key] = statsChan
	return statsChan
}
