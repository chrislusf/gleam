package agent

import (
	"bufio"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleInMemoryReadConnection(conn net.Conn, name string) {

	// println(name, "is waited to read")

	ch := as.inMemoryChannels.WaitForNamedDatasetShard(name)

	println(name, "start reading ...")

	var count int64

	writer := bufio.NewWriterSize(conn, 1024*16)

	// loop for every read
	for messageBytes := range ch {
		util.WriteMessage(writer, messageBytes)

		count += int64(len(messageBytes))
	}
	writer.Flush()
	println(name, "finish reading", count, "bytes")
}
