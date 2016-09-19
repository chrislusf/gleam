// Package netchan creates network channels. The network channels are managed by
// glow agent.
package netchan

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func DialReadChannel(wg *sync.WaitGroup, address string, channelName string, outChan chan []byte) error {

	readWriter, err := net.Dial("tcp", address)
	if err != nil {
		wg.Done()
		return fmt.Errorf("Fail to dial read %s: %v", address, err)
	}
	defer readWriter.Close()

	data, err := proto.Marshal(&cmd.ControlMessage{
		ReadRequest: &cmd.ReadRequest{
			Name: proto.String(channelName),
		},
	})

	util.WriteMessage(readWriter, data)

	util.ReaderToChannel(wg, channelName, readWriter, outChan, true, os.Stderr)

	return nil
}

func DialWriteChannel(wg *sync.WaitGroup, address string, channelName string, inChan chan []byte) error {

	readWriter, err := net.Dial("tcp", address)
	if err != nil {
		wg.Done()
		return fmt.Errorf("Fail to dial write %s: %v", address, err)
	}
	defer readWriter.Close()

	data, err := proto.Marshal(&cmd.ControlMessage{
		WriteRequest: &cmd.WriteRequest{
			Name: proto.String(channelName),
		},
	})

	util.WriteMessage(readWriter, data)

	util.ChannelToWriter(wg, channelName, inChan, readWriter, os.Stderr)

	return nil
}
