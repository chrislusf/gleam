package agent

import (
	"bufio"
	"io"
	"log"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalInMemoryWriteConnection(r io.Reader, writerName, channelName string, readerCount int) {

	ch := as.inMemoryChannels.CreateNamedDatasetShard(channelName, readerCount)
	defer func() {
		ch.incomingChannel.Writer.Close()
		ch.wg.Wait()
	}()

	log.Println(writerName, "start in memory writing to", channelName, "expected reader:", readerCount)

	writer := bufio.NewWriter(ch.incomingChannel.Writer)
	defer writer.Flush()

	buf := make([]byte, util.BUFFER_SIZE)
	io.CopyBuffer(writer, r, buf)

	log.Println(writerName, "finish in memory writing to", channelName)
}
