package agent

import (
	"bufio"
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalWriteConnection(reader io.Reader, writerName, channelName string, readerCount int) {

	dsStore := as.storageBackend.CreateNamedDatasetShard(channelName)

	log.Println(writerName, "start writing to", channelName, "expected reader:", readerCount)

	var count int64

	r := bufio.NewReaderSize(reader, util.BUFFER_SIZE)

	for {
		message, err := util.ReadMessage(r)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			count += int64(len(message))
			util.WriteMessage(dsStore, message)
			// println("agent recv:", string(message.Bytes()))
		} else {
			log.Printf("Failed to read message: %v", err)
		}
	}

	log.Println(writerName, "finish writing to", channelName, count, "bytes")

	util.WriteEOFMessage(dsStore)
}

func (as *AgentServer) handleDeleteDatasetShard(conn net.Conn,
	deleteRequest *msg.DeleteDatasetShardRequest) *msg.DeleteDatasetShardResponse {

	log.Println("deleting", *deleteRequest.Name)
	as.storageBackend.DeleteNamedDatasetShard(*deleteRequest.Name)

	return nil
}
