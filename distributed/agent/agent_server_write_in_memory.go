package agent

import (
	"bufio"
	"io"
	"log"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalInMemoryWriteConnection(r io.Reader, name string) {

	ch := as.inMemoryChannels.CreateNamedDatasetShard(name)

	println(name, "start writing.")

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

	println(name, "finish writing data", count, "bytes")
	close(ch)
}
