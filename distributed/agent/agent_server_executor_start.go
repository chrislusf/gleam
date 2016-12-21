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
	pb "github.com/chrislusf/gleam/idl/master_rpc"
	"github.com/chrislusf/gleam/msg"
	"github.com/golang/protobuf/proto"
	"github.com/kardianos/osext"
)

func (as *AgentServer) handleStart(conn net.Conn,
	startRequest *msg.StartRequest) *msg.StartResponse {

	// println("starting", startRequest.GetInstructions())
	reply := &msg.StartResponse{}
	stat := as.localExecutorManager.getExecutorStatus(*startRequest.GetInstructions().FlowHashCode)
	stat.RequestTime = time.Now()

	dir := path.Join(*as.Option.Dir, startRequest.GetDir())
	os.MkdirAll(dir, 0755)
	err := rsync.FetchFilesTo(startRequest.GetHost()+":"+strconv.Itoa(int(startRequest.GetPort())), dir)
	if err != nil {
		log.Printf("Failed to download file: %v", err)
		reply.Error = proto.String(err.Error())
		return reply
	}

	allocated := pb.ComputeResource{
		CpuCount: int32(startRequest.GetResource().GetCpuCount()),
		MemoryMb: int64(startRequest.GetResource().GetMemory()),
	}

	as.plusAllocated(allocated)
	defer as.minusAllocated(allocated)

	as.doCommand(conn, startRequest, stat, dir, reply)

	return reply
}

func (as *AgentServer) doCommand(
	conn net.Conn,
	startRequest *msg.StartRequest,
	stat *AgentExecutorStatus,
	dir string,
	reply *msg.StartResponse) (err error) {
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
		reply.Error = proto.String(err.Error())
	} else {
		reply.Pid = proto.Int32(int32(command.Process.Pid))
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
		reply.Error = proto.String(err.Error())
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
