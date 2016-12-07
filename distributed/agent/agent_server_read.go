package agent

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleReadConnection(conn net.Conn, readerName, channelName string) {

	log.Println("on disk", readerName, "waits for", channelName)

	dsStore := as.storageBackend.WaitForNamedDatasetShard(channelName)

	log.Println("on disk", readerName, "starts reading", channelName)

	var offset int64

	buf := make([]byte, 4)

	var count int64

	// loop for every read
	for {
		_, err := dsStore.ReadAt(buf, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read size from %s offset %d: %v", channelName, offset, err)
			}
			// println("got problem reading", channelName, offset, err.Error())
			break
		}

		var size int32
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &size)
		if size < 0 {
			// size == -1 means EOF
			break
		}

		// println("reading", channelName, offset, "size:", size)

		offset += 4
		messageBytes := make([]byte, size)
		_, err = dsStore.ReadAt(messageBytes, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read data from %s offset %d: %v", channelName, offset, err)
			}
			break
		}
		offset += int64(size)

		util.WriteMessage(conn, messageBytes)

		count += int64(size)

	}

	log.Println("on disk", readerName, "finish reading", channelName, count, "bytes")
}
