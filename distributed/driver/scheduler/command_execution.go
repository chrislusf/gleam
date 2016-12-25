package scheduler

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func NewStartRequest(name string, dir string, instructions *pb.InstructionSet,
	allocated *pb.ComputeResource, envs []string, host string, port int32) *pb.ControlMessage {

	request := &pb.ControlMessage{
		StartRequest: &pb.StartRequest{
			Instructions: instructions,
			Dir:          dir,
			Resource:     allocated,
			Host:         host,
			Port:         port,
			Name:         name,
		},
	}

	return request
}

func NewGetStatusRequest(requestId uint32) *pb.ControlMessage {
	return &pb.ControlMessage{
		GetStatusRequest: &pb.GetStatusRequest{
			StartRequestHash: requestId,
		},
	}
}

func NewStopRequest(requestId uint32) *pb.ControlMessage {
	return &pb.ControlMessage{
		StopRequest: &pb.StopRequest{
			StartRequestHash: requestId,
		},
	}
}

func NewDeleteDatasetShardRequest(name string) *pb.ControlMessage {
	return &pb.ControlMessage{
		DeleteDatasetShardRequest: &pb.DeleteDatasetShardRequest{
			Name: name,
		},
	}
}

func RemoteDirectExecute(server string, command *pb.ControlMessage) error {
	conn, err := getDirectCommandConnection(server)
	if err != nil {
		return err
	}
	defer conn.Close()

	return doExecute(server, conn, command)
}

// doExecute() sends a request and expects the output from the connection
func doExecute(server string, conn io.ReadWriteCloser, command *pb.ControlMessage) error {

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
	// the output from "gleam execute" should be just errors
	var b bytes.Buffer
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fmt.Fprintf(&b, "%s>%s\n", server, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Failed to scan output: %v", err)
	}

	if b.Len() == 0 {
		return nil
	}

	return errors.New(b.String())
}

func RemoteDirectCommand(server string, command *pb.ControlMessage) (response *pb.ControlMessage, err error) {
	conn, err := getDirectCommandConnection(server)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return doCommand(server, conn, command)
}

// doCommand() sends a request and expects a response object
func doCommand(server string, conn io.ReadWriteCloser, command *pb.ControlMessage) (response *pb.ControlMessage, err error) {

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
		return nil, fmt.Errorf("msg response: %v", err)
	}

	// unmarshal the bytes
	response = &pb.ControlMessage{}
	err = proto.Unmarshal(replyBytes, response)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling error: %v", err)
	}

	return response, err
}

func getDirectCommandConnection(target string) (io.ReadWriteCloser, error) {
	return net.Dial("tcp", target)
}
