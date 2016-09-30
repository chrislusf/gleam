package agent

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleReadConnection(conn net.Conn, name string) {

	// println(name, "is waited to read")

	dsStore := as.storageBackend.WaitForNamedDatasetShard(name)

	println(name, "start reading ...")

	writer := bufio.NewWriterSize(conn, 1024*16)

	var offset int64

	buf := make([]byte, 4)

	var count int64

	// loop for every read
	for {
		_, err := dsStore.ReadAt(buf, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read size from %s offset %d: %v", name, offset, err)
			}
			// println("got problem reading", name, offset, err.Error())
			break
		}

		var size int32
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &size)
		if size < 0 {
			// size == -1 means EOF
			break
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
			break
		}
		offset += int64(size)

		util.WriteMessage(writer, messageBytes)

		count += int64(size)

	}

	writer.Flush()

	println(name, "finish reading", count, "bytes")
}
