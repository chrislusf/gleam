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

	log.Printf("on disk %s waits for %s", readerName, channelName)

	dsStore := as.storageBackend.WaitForNamedDatasetShard(channelName)

	log.Printf("on disk %s starts reading %s", readerName, channelName)

	var offset int64
	var err error

	var size int32
	sizeBuf := make([]byte, 4)
	sizeReader := bytes.NewReader(sizeBuf)

	var count int64
	messageBytesCache := make([]byte, util.BUFFER_SIZE)
	var messageBytes []byte

	messageWriter := util.NewBufferedMessageWriter(conn, util.BUFFER_SIZE)
	// loop for every read
	for {
		_, err = dsStore.ReadAt(sizeBuf, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read size from %s offset %d: %v", channelName, offset, err)
			}
			// println("got problem reading", channelName, offset, err.Error())
			break
		}

		sizeReader.Reset(sizeBuf)
		binary.Read(sizeReader, binary.LittleEndian, &size)
		if size == int32(util.MessageControlEOF) {
			break
		}

		// println("reading", channelName, offset, "size:", size)

		offset += 4
		if size > util.BUFFER_SIZE {
			messageBytes = make([]byte, size)
		} else {
			messageBytes = messageBytesCache[0:size]
		}
		_, err = dsStore.ReadAt(messageBytes, offset)
		if err != nil {
			// connection is closed
			if err != io.EOF {
				log.Printf("Read data from %s offset %d: %v", channelName, offset, err)
			}
			break
		}
		offset += int64(size)

		err = messageWriter.WriteMessage(messageBytes)
		if err != nil {
			log.Printf("%s failed to receive %s at %d: %v", readerName, channelName, offset, err)
			break
		}

		count += int64(size)

	}
	messageWriter.Flush()

	if err != nil {
		log.Printf("on disk %s finished reading %s %d bytes error: %v", readerName, channelName, count, err)
	} else {
		log.Printf("on disk %s finished reading %s %d bytes", readerName, channelName, count)
	}
}
