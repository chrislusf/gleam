package agent

import (
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleReadConnection(conn net.Conn, name string) {

	dsStore := as.storageBackend.WaitForNamedDatasetShard(name)

	// println(name, "start reading from:", offset)

	var offset int64

	buf := make([]byte, 4)

	// loop for every read
	for {
		_, err := dsStore.ReadAt(buf, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read size from %s offset %d: %v", name, offset, err)
			}
			// println("got problem reading", name, offset, err.Error())
			return
		}

		offset += 4

		size := util.BytesToUint32(buf)

		// println("reading", name, offset, "size:", size)

		messageBytes := make([]byte, size)
		_, err = dsStore.ReadAt(messageBytes, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read data from %s offset %d: %v", name, offset, err)
			}
			return
		}
		offset += int64(size)

		util.WriteMessage(conn, messageBytes)

	}

	// wait for the close ack
	util.ReadMessage(conn)

}
