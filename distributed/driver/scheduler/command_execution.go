package scheduler

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func NewGetStatusRequest(requestId uint32) *pb.ControlMessage {
	return &pb.ControlMessage{
		GetStatusRequest: &pb.GetStatusRequest{
			StartRequestHash: requestId,
		},
	}
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
