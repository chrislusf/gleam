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

func (as *AgentServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterGleamAgentServer(grpcServer, as)
	grpcServer.Serve(listener)
}

func (as *AgentServer) SendFileResource(stream pb.GleamAgent_SendFileResourceServer) error {
	as.receiveFileResourceLock.Lock()
	defer as.receiveFileResourceLock.Unlock()

	request, err := stream.Recv()
	if err != nil {
		return err
	}

	dir := path.Join(*as.Option.Dir, fmt.Sprintf("%d", request.GetFlowHashCode()), request.GetDir())
	os.MkdirAll(dir, 0755)

	toFile := filepath.Join(dir, request.GetName())
	hasSameHash := false
	if toFileHash, err := resource.GenerateFileHash(toFile); err == nil {
		hasSameHash = toFileHash.Hash == request.GetHash()
	}

	if err := stream.Send(&pb.FileResourceResponse{hasSameHash, true}); err != nil {
		return err
	}

	if hasSameHash {
		return nil
	}

	f, err := os.OpenFile(toFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = f.Write(request.GetContent())
		if err != nil {
			log.Printf("Write file error: ", err)
			return err
		}
	}
}

// Execute executes a request and stream stdout and stderr back
func (as *AgentServer) Execute(request *pb.ExecutionRequest, stream pb.GleamAgent_ExecuteServer) error {

	dir := path.Join(*as.Option.Dir, fmt.Sprintf("%d", request.GetInstructionSet().GetFlowHashCode()), request.GetDir())
	os.MkdirAll(dir, 0755)

	allocated := *request.GetResource()

	as.plusAllocated(allocated)
	defer as.minusAllocated(allocated)

	request.InstructionSet.AgentAddress = fmt.Sprintf(":%d", *as.Option.Port)

	key := fmt.Sprintf(
		"%d-%s",
		request.InstructionSet.FlowHashCode,
		request.InstructionSet.Name,
	)
	statsChan := make(chan *pb.ExecutionStat)
	statsChanMapRWMutex.Lock()
	statsChanMap[key] = statsChan
	statsChanMapRWMutex.Unlock()

	return as.executeCommand(stream, request, dir, statsChan)

}

// Collect stat from "gleam execute" process
func (as *AgentServer) CollectExecutionStatistics(stream pb.GleamAgent_CollectExecutionStatisticsServer) error {
	var statsChan chan *pb.ExecutionStat

	for {
		stats, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if statsChan == nil {
			key := fmt.Sprintf(
				"%d-%s",
				stats.FlowHashCode,
				stats.Name,
			)
			statsChanMapRWMutex.RLock()
			statsChan = statsChanMap[key]
			statsChanMapRWMutex.RUnlock()
		}

		statsChan <- stats
		// fmt.Printf("stats: %+v\n", stats)
	}

}

// Delete deletes a particular dataset shard
func (as *AgentServer) Delete(ctx context.Context, deleteRequest *pb.DeleteDatasetShardRequest) (*pb.DeleteDatasetShardResponse, error) {

	log.Println("deleting", deleteRequest.Name)
	as.storageBackend.DeleteNamedDatasetShard(deleteRequest.Name)
	as.inMemoryChannels.Cleanup(deleteRequest.Name)

	return &pb.DeleteDatasetShardResponse{}, nil
}

func (as *AgentServer) plusAllocated(allocated pb.ComputeResource) {
	as.allocatedResourceLock.Lock()
	defer as.allocatedResourceLock.Unlock()
	*as.allocatedResource = as.allocatedResource.Plus(allocated)
}

func (as *AgentServer) minusAllocated(allocated pb.ComputeResource) {
	as.allocatedResourceLock.Lock()
	defer as.allocatedResourceLock.Unlock()
	*as.allocatedResource = as.allocatedResource.Minus(allocated)
}
