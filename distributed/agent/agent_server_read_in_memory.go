package agent

import (
	"bufio"
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleInMemoryReadConnection(conn net.Conn, readerName, channelName string) {

	log.Println("in memory", readerName, "waits for", channelName)

	ch := as.inMemoryChannels.WaitForNamedDatasetShard(channelName)

	if ch == nil {
		log.Println("in memory", readerName, "read an empty", channelName)
		return
	}

	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	log.Println("in memory", readerName, "start reading", channelName)
	buf := make([]byte, util.BUFFER_SIZE)
	count, err := io.CopyBuffer(writer, ch.Reader, buf)

	log.Printf("in memory %s finish reading %s bytes:%d %v", readerName, channelName, count, err)
}
