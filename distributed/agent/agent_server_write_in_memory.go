package agent

import (
	"bufio"
	"io"
	"log"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalInMemoryWriteConnection(r io.Reader, writerName, channelName string, readerCount int) {

	ch := as.inMemoryChannels.CreateNamedDatasetShard(channelName, readerCount)
	defer as.inMemoryChannels.Cleanup(channelName)
	defer close(ch)

	// println(writerName, "start in memory writing to", channelName, "expected reader:", readerCount)

	var count int64

	reader := bufio.NewReader(r)

	for {
		message, err := util.ReadMessage(reader)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			count += int64(len(message))
			ch <- message
			// println("agent recv:", string(message.Bytes()))
		} else {
			log.Printf("Failed to read message: %v", err)
		}
	}

	// println(writerName, "finish writing to", channelName, count, "bytes")
}
