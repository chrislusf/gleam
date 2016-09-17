package agent

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
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
		as.adjustArgs(startRequest.Args, startRequest.GetHashCode())...,
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

func (as *AgentServer) adjustArgs(args []string, requestId uint32) (ret []string) {
	ret = []string{"-glow.request.id", fmt.Sprintf("%d", requestId)}

	if as.Option.CertFiles.IsEnabled() {
		var cleanedArgs []string
		for _, arg := range args {
			if strings.Contains(arg, "=") {
				cleanedArgs = append(cleanedArgs, strings.SplitN(arg, "=", 2)...)
			} else {
				cleanedArgs = append(cleanedArgs, arg)
			}
		}
		for i := 0; i < len(cleanedArgs); i++ {
			ret = append(ret, cleanedArgs[i])
			switch cleanedArgs[i] {
			case "-cert.file":
				ret = append(ret, as.Option.CertFiles.CertFile)
				i++
			case "-key.file":
				ret = append(ret, as.Option.CertFiles.KeyFile)
				i++
			case "-ca.file":
				ret = append(ret, as.Option.CertFiles.CaFile)
				i++
			}
		}
	} else {
		ret = append(ret, args...)
	}

	// fmt.Printf("cmd: %v\n", ret)
	return
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
