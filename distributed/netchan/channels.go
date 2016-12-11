// Package netchan creates network channels. The network channels are managed by
// glow agent.
package netchan

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func DialReadChannel(wg *sync.WaitGroup, readerName string, address string, channelName string, onDisk bool, outChan io.WriteCloser) error {

	conn, err := net.Dial("tcp", address)
	if err != nil {
		wg.Done()
		return fmt.Errorf("Fail to dial read %s: %v", address, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Time{})
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
	}

	data, err := proto.Marshal(&msg.ControlMessage{
		IsOnDiskIO: proto.Bool(onDisk),
		ReadRequest: &msg.ReadRequest{
			ChannelName: proto.String(channelName),
			ReaderName:  proto.String(readerName),
		},
	})

	if err = util.WriteMessage(conn, data); err != nil {
		wg.Done()
		return err
	}

	return util.ReaderToChannel(wg, channelName, conn, outChan, true, os.Stderr)
}

func DialWriteChannel(wg *sync.WaitGroup, writerName string, address string, channelName string, onDisk bool, inChan io.Reader, readerCount int) error {

	conn, err := net.Dial("tcp", address)
	if err != nil {
		wg.Done()
		return fmt.Errorf("Fail to dial write %s: %v", address, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Time{})
	if c, ok := conn.(*net.TCPConn); ok {
		c.SetKeepAlive(true)
	}

	data, err := proto.Marshal(&msg.ControlMessage{
		IsOnDiskIO: proto.Bool(onDisk),
		WriteRequest: &msg.WriteRequest{
			ChannelName: proto.String(channelName),
			ReaderCount: proto.Int32(int32(readerCount)),
			WriterName:  proto.String(writerName),
		},
	})

	if err = util.WriteMessage(conn, data); err != nil {
		wg.Done()
		return err
	}

	return util.ChannelToWriter(wg, channelName, inChan, conn, os.Stderr)

}
