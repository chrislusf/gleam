package agent

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/chrislusf/gleam/distributed/rsync"
	"github.com/chrislusf/gleam/pb"
	"github.com/golang/protobuf/proto"
	"github.com/kardianos/osext"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (as *AgentServer) serveGrpc(listener net.Listener) {
	grpcServer := grpc.NewServer()
	pb.RegisterGleamAgentServer(grpcServer, as)
	grpcServer.Serve(listener)
}

// Execute executes a request and stream stdout and stderr back
func (as *AgentServer) Execute(request *pb.ExecutionRequest, stream pb.GleamAgent_ExecuteServer) error {

	stat := as.localExecutorManager.getExecutorStatus(request.GetInstructions().FlowHashCode)
	stat.RequestTime = time.Now()

	dir := path.Join(*as.Option.Dir, request.GetDir())
	os.MkdirAll(dir, 0755)
	err := rsync.FetchFilesTo(request.GetHost()+":"+strconv.Itoa(int(request.GetPort())), dir)
	if err != nil {
		if sendErr := stream.Send(&pb.ExecutionResponse{
			Error: []byte(fmt.Sprintf("Failed to download file: %v", err)),
		}); sendErr != nil {
			return sendErr
		}
		return err
	}

	allocated := *request.GetResource()

	as.plusAllocated(allocated)
	defer as.minusAllocated(allocated)

	return as.executeCommand(stream, request, dir, stat)

}

func (as *AgentServer) executeCommand(
	stream pb.GleamAgent_ExecuteServer,
	startRequest *pb.ExecutionRequest,
	dir string,
	stat *AgentExecutorStatus,
) (err error) {
	// start the command
	executableFullFilename, _ := osext.Executable()
	stat.StartTime = time.Now()
	command := exec.Command(
		executableFullFilename,
		"execute",
		"--note",
		startRequest.GetName(),
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		log.Printf("Failed to create stdin pipe: %v", err)
		return
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		log.Printf("Failed to create stdout pipe: %v", err)
		return
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		log.Printf("Failed to create stderr pipe: %v", err)
		return
	}
	// msg.Env = startRequest.Envs
	command.Dir = dir

	err = command.Start()
	if err != nil {
		log.Printf("Failed to start command %s under %s: %v",
			command.Path, command.Dir, err)
		return err
	}
	stat.Process = command.Process

	var wg sync.WaitGroup
	wg.Add(1)
	go streamOutput(&wg, stream, stdout)
	wg.Add(1)
	go streamError(&wg, stream, stderr)

	// send instruction set to executor
	msgMessageBytes, err := proto.Marshal(startRequest.GetInstructions())
	if err != nil {
		log.Printf("Failed to marshal command %s: %v",
			startRequest.GetInstructions().String(), err)
		return err
	}
	_, err = stdin.Write(msgMessageBytes)
	if err != nil {
		log.Printf("Failed to write command: %v", err)
		return err
	}
	err = stdin.Close()
	if err != nil {
		log.Printf("Failed to close command: %v", err)
		return err
	}

	// wait for finish
	err = command.Wait()
	if err != nil {
		log.Printf("Failed to run command: %v", err)
	}
	stat.StopTime = time.Now()

	wg.Wait()
	// log.Printf("Finish command %+v", msg)

	return err
}

// Delete deletes a particular dataset shard
func (as *AgentServer) Delete(ctx context.Context, deleteRequest *pb.DeleteDatasetShardRequest) (*pb.DeleteDatasetShardResponse, error) {
	log.Println("deleting", deleteRequest.Name)
	as.storageBackend.DeleteNamedDatasetShard(deleteRequest.Name)

	return &pb.DeleteDatasetShardResponse{}, nil
}

func streamOutput(wg *sync.WaitGroup, stream pb.GleamAgent_ExecuteServer, reader io.Reader) {
	defer wg.Done()

	buffer := make([]byte, 1024)
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			return
		}
		if n == 0 {
			continue
		}

		if sendErr := stream.Send(&pb.ExecutionResponse{
			Output: buffer[0:n],
		}); sendErr != nil {
			log.Printf("Failed to send output response: %v", sendErr)
			break
		}
	}
}

func streamError(wg *sync.WaitGroup, stream pb.GleamAgent_ExecuteServer, reader io.Reader) {
	defer wg.Done()

	buffer := make([]byte, 1024)
	for {
		n, err := reader.Read(buffer)
		if err == io.EOF {
			break
		}
		if n == 0 {
			continue
		}

		if sendErr := stream.Send(&pb.ExecutionResponse{
			Error: buffer[0:n],
		}); sendErr != nil {
			log.Printf("Failed to send error response: %v", sendErr)
			break
		}
	}
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
