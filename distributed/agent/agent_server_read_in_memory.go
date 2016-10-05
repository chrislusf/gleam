package agent

import (
	"bufio"
	"io"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleInMemoryReadConnection(conn net.Conn, readerName, channelName string) {

	ch := as.inMemoryChannels.WaitForNamedDatasetShard(channelName)

	// println(readerName, "start reading", channelName)
	writer := bufio.NewWriterSize(conn, util.BUFFER_SIZE)
	reader := bufio.NewReaderSize(ch.Reader, util.BUFFER_SIZE)
	io.Copy(writer, reader)
	writer.Flush()

	// println(readerName, "finish reading", channelName)
}
