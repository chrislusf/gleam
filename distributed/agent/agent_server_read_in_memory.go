package agent

import (
	"io"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleInMemoryReadConnection(conn net.Conn, readerName, channelName string) {

	// println(readerName, "waits for", channelName)

	ch := as.inMemoryChannels.WaitForNamedDatasetShard(channelName)

	// println(readerName, "start reading", channelName)
	buf := make([]byte, util.BUFFER_SIZE)
	io.CopyBuffer(conn, ch.Reader, buf)

	// println(readerName, "finish reading", channelName)
}
