package agent

import (
	"bufio"
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalWriteConnection(reader io.Reader, writerName, channelName string, readerCount int) {

	dsStore := as.storageBackend.CreateNamedDatasetShard(channelName)

	// println(writerName, "start writing to", channelName, "expected reader:", readerCount)

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

	// println(writerName, "finish writing to", channelName, count, "bytes")
	util.WriteEOFMessage(dsStore)
}

func (as *AgentServer) handleDeleteDatasetShard(conn net.Conn,
	deleteRequest *cmd.DeleteDatasetShardRequest) *cmd.DeleteDatasetShardResponse {

	as.storageBackend.DeleteNamedDatasetShard(*deleteRequest.Name)

	return nil
}
