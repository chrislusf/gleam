package agent

import (
	"bufio"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleInMemoryReadConnection(conn net.Conn, readerName, channelName string) {

	// println(readerName, "start reading", channelName)

	ch := as.inMemoryChannels.WaitForNamedDatasetShard(channelName)

	var count int64

	writer := bufio.NewWriterSize(conn, util.BUFFER_SIZE)

	// loop for every read
	for messageBytes := range ch {
		util.WriteMessage(writer, messageBytes)

		count += int64(len(messageBytes))
	}
	writer.Flush()
	// println(readerName, "finish reading", channelName, count, "bytes")
}
