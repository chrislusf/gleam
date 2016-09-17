package agent

import (
	"io"
	"net"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalWriteConnection(r io.Reader, name string) {

	dsStore := as.storageBackend.CreateNamedDatasetShard(name)

	// println(name, "start writing.")

	for {
		message, err := util.ReadMessage(r)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			util.WriteMessage(dsStore, message)
			// println("agent recv:", string(message.Bytes()))
		}
	}
}

func (as *AgentServer) handleDeleteDatasetShard(conn net.Conn,
	deleteRequest *cmd.DeleteDatasetShardRequest) *cmd.DeleteDatasetShardResponse {

	as.storageBackend.DeleteNamedDatasetShard(*deleteRequest.Name)

	return nil
}
