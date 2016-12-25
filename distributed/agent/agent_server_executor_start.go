package agent

import (
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/chrislusf/gleam/distributed/rsync"
	"github.com/chrislusf/gleam/pb"
	"github.com/golang/protobuf/proto"
	"github.com/kardianos/osext"
)

func (as *AgentServer) handleStart(conn net.Conn,
	startRequest *pb.StartRequest) *pb.StartResponse {

	// println("starting", startRequest.GetInstructions())
	reply := &pb.StartResponse{}
	stat := as.localExecutorManager.getExecutorStatus(startRequest.GetInstructions().FlowHashCode)
	stat.RequestTime = time.Now()

	dir := path.Join(*as.Option.Dir, startRequest.GetDir())
	os.MkdirAll(dir, 0755)
	err := rsync.FetchFilesTo(startRequest.GetHost()+":"+strconv.Itoa(int(startRequest.GetPort())), dir)
	if err != nil {
		log.Printf("Failed to download file: %v", err)
		reply.Error = err.Error()
		return reply
	}

	allocated := *startRequest.GetResource()

	as.plusAllocated(allocated)
	defer as.minusAllocated(allocated)

	as.doCommand(conn, startRequest, stat, dir, reply)

	return reply
}

func (as *AgentServer) doCommand(
	conn net.Conn,
	startRequest *pb.StartRequest,
	stat *AgentExecutorStatus,
	dir string,
	reply *pb.StartResponse) (err error) {
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
	// msg.Env = startRequest.Envs
	command.Dir = dir
	// the "gleam execute" stdout actually should always be empty
	command.Stdout = conn
	// the "gleam execute" stderr goes directly back to the driver
	command.Stderr = conn
	err = command.Start()
	if err != nil {
		log.Printf("Failed to start command %s under %s: %v",
			command.Path, command.Dir, err)
		reply.Error = err.Error()
	} else {
		reply.Pid = int32(command.Process.Pid)
	}
	stat.Process = command.Process

	// send instruction set to executor
	msgMessageBytes, err := proto.Marshal(startRequest.GetInstructions())
	if err != nil {
		log.Printf("Failed to marshal command %s: %v",
			startRequest.GetInstructions().String(), err)
	}
	_, err = stdin.Write(msgMessageBytes)
	if err != nil {
		log.Printf("Failed to write command: %v", err)
	}
	err = stdin.Close()
	if err != nil {
		log.Printf("Failed to close command: %v", err)
	}

	// wait for finish
	err = command.Wait()
	if err != nil {
		reply.Error = err.Error()
	}
	// println("finished", startRequest.GetInstructions().String())
	stat.StopTime = time.Now()

	// log.Printf("Finish command %+v", msg)

	return err
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
