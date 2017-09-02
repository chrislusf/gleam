package agent

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	statsChanMap        = make(map[string]chan *pb.ExecutionStat)
	statsChanMapRWMutex sync.RWMutex
)

func getStatsChan(flowHashCode uint32, stepId int32, taskId int32) chan *pb.ExecutionStat {
	key := fmt.Sprintf("%d-%d-%d", flowHashCode, stepId, taskId)
	statsChanMapRWMutex.RLock()
	defer statsChanMapRWMutex.RUnlock()
	return statsChanMap[key]
}

func deleteStatsChanByInstructionSet(instructionSet *pb.InstructionSet) {
	deleteStatsChan(instructionSet.FlowHashCode,
		instructionSet.Instructions[0].GetStepId(),
		instructionSet.Instructions[0].GetTaskId())
}

func deleteStatsChan(flowHashCode uint32, stepId int32, taskId int32) {
	key := fmt.Sprintf("%d-%d-%d", flowHashCode, stepId, taskId)
	statsChanMapRWMutex.RLock()
	defer statsChanMapRWMutex.RUnlock()
	delete(statsChanMap, key)
}

func createStatsChanByInstructionSet(instructionSet *pb.InstructionSet) chan *pb.ExecutionStat {
	statsChan := make(chan *pb.ExecutionStat)
	key := fmt.Sprintf("%d-%d-%d",
		instructionSet.FlowHashCode,
		instructionSet.Instructions[0].GetStepId(),
		instructionSet.Instructions[0].GetTaskId())
	statsChanMapRWMutex.RLock()
	defer statsChanMapRWMutex.RUnlock()
	statsChanMap[key] = statsChan
	return statsChan
}
