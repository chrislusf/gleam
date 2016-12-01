package agent

import (
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalWriteConnection(reader io.Reader, writerName, channelName string, readerCount int) {

	dsStore := as.storageBackend.CreateNamedDatasetShard(channelName)

	log.Println("on disk", writerName, "start writing", channelName, "expected reader:", readerCount)

	var count int64

	messageWriter := util.NewBufferedMessageWriter(dsStore, 54)

	for {
		message, err := util.ReadMessage(reader)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			count += int64(len(message))
			messageWriter.WriteMessage(message)
			// println("agent recv:", string(message.Bytes()))
		} else {
			log.Printf("Failed to read message: %v", err)
		}
	}

	messageWriter.Flush()
	util.WriteEOFMessage(dsStore)

	log.Println("on disk", writerName, "finish writing", channelName, count, "bytes")

}

func (as *AgentServer) handleDeleteDatasetShard(conn net.Conn,
	deleteRequest *msg.DeleteDatasetShardRequest) *msg.DeleteDatasetShardResponse {

	log.Println("deleting", *deleteRequest.Name)
	as.storageBackend.DeleteNamedDatasetShard(*deleteRequest.Name)

	return nil
}
