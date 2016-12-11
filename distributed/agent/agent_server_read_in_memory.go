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

	if err == nil {
		if ch.Error != nil {
			log.Printf("in memory %s failed because writing to %s failed: %d %v", readerName, channelName, count, ch.Error)
		} else {
			log.Printf("in memory %s finished reading %s bytes:%d", readerName, channelName, count)
		}
	} else {
		log.Printf("in memory %s failed reading %s bytes:%d %v", readerName, channelName, count, err)
	}

}
