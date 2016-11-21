package agent

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalInMemoryWriteConnection(r io.Reader, writerName, channelName string, readerCount int) {

	ch := as.inMemoryChannels.CreateNamedDatasetShard(channelName, readerCount)
	defer func() {
		ch.incomingChannel.Writer.Close()
		ch.wg.Wait()
		as.inMemoryChannels.Cleanup(channelName)
	}()

	log.Println(writerName, "start in memory writing to", channelName, "expected reader:", readerCount)

	buf := make([]byte, util.BUFFER_SIZE)
	io.CopyBuffer(ch.incomingChannel.Writer, r, buf)

	log.Println(writerName, "finish in memory writing to", channelName)
}
