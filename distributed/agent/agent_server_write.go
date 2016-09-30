package agent

import (
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalWriteConnection(reader io.Reader, name string) {

	dsStore := as.storageBackend.CreateNamedDatasetShard(name)

	println(name, "start writing.")

	var count int64

	for {
		message, err := util.ReadMessage(reader)
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

	println(name, "finish writing data", count, "bytes")
	util.WriteEOFMessage(dsStore)
}

func (as *AgentServer) handleDeleteDatasetShard(conn net.Conn,
	deleteRequest *cmd.DeleteDatasetShardRequest) *cmd.DeleteDatasetShardResponse {

	as.storageBackend.DeleteNamedDatasetShard(*deleteRequest.Name)

	return nil
}
