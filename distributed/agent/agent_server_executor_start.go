package agent

import (
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/glow/driver/rsync"
	"github.com/chrislusf/glow/resource"
	"github.com/golang/protobuf/proto"
)

func (as *AgentServer) handleStart(conn net.Conn,
	startRequest *cmd.StartRequest) *cmd.StartResponse {
	reply := &cmd.StartResponse{}
	stat := as.localExecutorManager.getExecutorStatus(startRequest.GetHashCode())
	stat.RequestTime = time.Now()

	dir := path.Join(*as.Option.Dir, startRequest.GetDir())
	os.MkdirAll(dir, 0755)
	err := rsync.FetchFilesTo(startRequest.GetHost()+":"+strconv.Itoa(int(startRequest.GetPort())), dir)
	if err != nil {
		log.Printf("Failed to download file: %v", err)
		reply.Error = proto.String(err.Error())
		return reply
	}

	allocated := resource.ComputeResource{
		CPUCount: int(startRequest.GetResource().GetCpuCount()),
		MemoryMB: int64(startRequest.GetResource().GetMemory()),
	}

	as.plusAllocated(allocated)
	defer as.minusAllocated(allocated)

	stat.StartTime = time.Now()
	cmd := exec.Command(
		startRequest.GetPath(),
		startRequest.Args...,
	)
	cmd.Env = startRequest.Envs
	cmd.Dir = dir
	cmd.Stdout = conn
	cmd.Stderr = conn
	err = cmd.Start()
	if err != nil {
		log.Printf("Failed to start command %s under %s: %v",
			cmd.Path, cmd.Dir, err)
		reply.Error = proto.String(err.Error())
	} else {
		reply.Pid = proto.Int32(int32(cmd.Process.Pid))
	}
	stat.Process = cmd.Process

	cmd.Wait()
	stat.StopTime = time.Now()

	// log.Printf("Finish command %v", cmd)

	return reply
}

func (as *AgentServer) plusAllocated(allocated resource.ComputeResource) {
	as.allocatedResourceLock.Lock()
	defer as.allocatedResourceLock.Unlock()
	*as.allocatedResource = as.allocatedResource.Plus(allocated)
}

func (as *AgentServer) minusAllocated(allocated resource.ComputeResource) {
	as.allocatedResourceLock.Lock()
	defer as.allocatedResourceLock.Unlock()
	*as.allocatedResource = as.allocatedResource.Minus(allocated)
}
