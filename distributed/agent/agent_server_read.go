package agent

import (
	"bytes"
	"encoding/binary"
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

		var size int32
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &size)
		if size < 0 {
			// size == -1 means EOF
			return
		}

		// println("reading", name, offset, "size:", size)

		offset += 4
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

}
