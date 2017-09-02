package agent

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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
			break
		}
		if err != nil {
			log.Printf("Receiving file %s error: %v", toFile, err)
			return err
		}
		_, err = f.Write(request.GetContent())
		if err != nil {
			log.Printf("Write file error: %v", err)
			return err
		}
	}

	// ack
	if err := stream.Send(&pb.FileResourceResponse{hasSameHash, true}); err != nil {
		return err
	}

	return nil

}

// Cleanup remove all files related to a particular flow
func (as *AgentServer) Cleanup(ctx context.Context, cleanupRequest *pb.CleanupRequest) (*pb.CleanupResponse, error) {

	log.Println("cleaning up", cleanupRequest.GetFlowHashCode())
	dir := path.Join(*as.Option.Dir, fmt.Sprintf("%d", cleanupRequest.GetFlowHashCode()))
	os.RemoveAll(dir)

	return &pb.CleanupResponse{}, nil
}

// Execute executes a request and stream stdout and stderr back
func (as *AgentServer) Execute(request *pb.ExecutionRequest, stream pb.GleamAgent_ExecuteServer) error {

	dir := path.Join(*as.Option.Dir, fmt.Sprintf("%d", request.GetInstructionSet().GetFlowHashCode()), request.GetDir())
	os.MkdirAll(dir, 0755)

	allocated := *request.GetResource()

	as.plusAllocated(allocated)
	defer as.minusAllocated(allocated)

	request.InstructionSet.AgentAddress = fmt.Sprintf("%s:%d", *as.Option.Host, *as.Option.Port)

	statsChan := createStatsChanByInstructionSet(request.InstructionSet)

	defer deleteStatsChanByInstructionSet(request.InstructionSet)

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
			statsChan = getStatsChan(stats.FlowHashCode, stats.Stats[0].GetStepId(), stats.Stats[0].GetTaskId())
		}

		statsChan <- stats
		// fmt.Printf("received stats: %+v\n", stats)
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
	as.allocatedHasChanges = true
	*as.allocatedResource = as.allocatedResource.Plus(allocated)
}

func (as *AgentServer) minusAllocated(allocated pb.ComputeResource) {
	as.allocatedResourceLock.Lock()
	defer as.allocatedResourceLock.Unlock()
	as.allocatedHasChanges = true
	*as.allocatedResource = as.allocatedResource.Minus(allocated)
}
