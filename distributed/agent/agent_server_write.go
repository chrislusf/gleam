package agent

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalWriteConnection(reader io.Reader, writerName, channelName string, readerCount int) {

	dsStore := as.storageBackend.CreateNamedDatasetShard(channelName)

	log.Println("on disk", writerName, "start writing", channelName, "expected reader:", readerCount)

	var count int64

	messageWriter := util.NewBufferedMessageWriter(dsStore, util.BUFFER_SIZE)

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
			log.Printf("on disk %s Failed to write to %s: %v", writerName, channelName, err)
		}
	}

	messageWriter.Flush()
	util.WriteEOFMessage(dsStore)

	log.Println("on disk", writerName, "finish writing", channelName, count, "bytes")

}
