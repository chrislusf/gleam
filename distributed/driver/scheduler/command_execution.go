package scheduler

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"time"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/distributed/resource/service_discovery/client"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func NewStartRequest(dir string, instructions *cmd.InstructionSet, allocated resource.ComputeResource, envs []string, host string, port int32) *cmd.ControlMessage {
	request := &cmd.ControlMessage{
		StartRequest: &cmd.StartRequest{
			Instructions: instructions,
			Dir:          proto.String(dir),
			Resource: &cmd.ComputeResource{
				CpuCount: proto.Int32(int32(allocated.CPUCount)),
				CpuLevel: proto.Int32(int32(allocated.CPULevel)),
				Memory:   proto.Int32(int32(allocated.MemoryMB)),
				GpuCount: proto.Int32(int32(allocated.GPUCount)),
				GpuLevel: proto.Int32(int32(allocated.GPULevel)),
			},
			Host: proto.String(host),
			Port: proto.Int32(port),
		},
	}

	return request
}

func NewGetStatusRequest(requestId uint32) *cmd.ControlMessage {
	return &cmd.ControlMessage{
		GetStatusRequest: &cmd.GetStatusRequest{
			StartRequestHash: proto.Uint32(requestId),
		},
	}
}

func NewStopRequest(requestId uint32) *cmd.ControlMessage {
	return &cmd.ControlMessage{
		StopRequest: &cmd.StopRequest{
			StartRequestHash: proto.Uint32(requestId),
		},
	}
}

func NewDeleteDatasetShardRequest(name string) *cmd.ControlMessage {
	return &cmd.ControlMessage{
		DeleteDatasetShardRequest: &cmd.DeleteDatasetShardRequest{
			Name: proto.String(name),
		},
	}
}

func RemoteDirectExecute(server string, command *cmd.ControlMessage) error {
	conn, err := getDirectCommandConnection(server)
	if err != nil {
		return err
	}
	defer conn.Close()

	return doExecute(server, conn, command)
}

// doExecute() sends a request and expects the output from the connection
func doExecute(server string, conn io.ReadWriteCloser, command *cmd.ControlMessage) error {

	// serialize the commend
	data, err := proto.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshaling execute request error: %v", err)
	}

	// send the command
	if err = util.WriteMessage(conn, data); err != nil {
		return fmt.Errorf("failed to write to %s: %v", server, err)
	}

	// println("command sent")

	// read output and print it to stdout
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fmt.Printf("%s>%s\n", server, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Failed to scan output: %v", err)
	}

	return err
}

func RemoteDirectCommand(server string, command *cmd.ControlMessage) (response *cmd.ControlMessage, err error) {
	conn, err := getDirectCommandConnection(server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return doCommand(server, conn, command)
}

// doCommand() sends a request and expects a response object
func doCommand(server string, conn io.ReadWriteCloser, command *cmd.ControlMessage) (response *cmd.ControlMessage, err error) {

	// serialize the commend
	data, err := proto.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshaling command error: %v", err)
	}

	// send the command
	if err = util.WriteMessage(conn, data); err != nil {
		return nil, fmt.Errorf("failed to write command to %s: %v", server, err)
	}

	// println("command sent")

	// read response
	replyBytes, err := ioutil.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("cmd response: %v", err)
	}

	// unmarshal the bytes
	response = &cmd.ControlMessage{}
	err = proto.Unmarshal(replyBytes, response)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling error: %v", err)
	}

	return response, err
}

func getCommandConnection(leader string, agentName string) (io.ReadWriteCloser, error) {
	l := client.NewNameServiceProxy(leader)

	// looking for the agentName
	var target string
	for {
		locations := l.Find(agentName)
		if len(locations) > 0 {
			target = locations[0]
		}
		if target != "" {
			break
		} else {
			time.Sleep(time.Second)
			print("z")
		}
	}

	return getDirectCommandConnection(target)
}

func getDirectCommandConnection(target string) (io.ReadWriteCloser, error) {
	return net.Dial("tcp", target)
}
