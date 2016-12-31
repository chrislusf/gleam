package agent

import (
	"bufio"
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleInMemoryReadConnection(conn net.Conn, readerName, channelName string) {

	log.Printf("in memory %s waits for %s", readerName, channelName)

	ch := as.inMemoryChannels.WaitForNamedDatasetShard(channelName)

	if ch == nil {
		log.Printf("in memory %s read an empty %s", readerName, channelName)
		return
	}

	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	log.Printf("in memory %s start reading %s", readerName, channelName)
	buf := make([]byte, util.BUFFER_SIZE)
	count, err := io.CopyBuffer(writer, ch.Reader, buf)

	if err == nil {
		if ch.Error != nil {
			log.Printf("in memory %s failed because writing to %s failed: %d %v", readerName, channelName, count, ch.Error)
		} else {
			log.Printf("in memory %s finished reading %s %d bytes", readerName, channelName, count)
		}
	} else {
		log.Printf("in memory %s failed reading %s %d bytes %v", readerName, channelName, count, err)
	}

}
