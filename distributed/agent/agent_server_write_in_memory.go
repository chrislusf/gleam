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

	log.Println("in memory", writerName, "start writing", channelName, "expected reader:", readerCount)

	writer := bufio.NewWriter(ch.incomingChannel.Writer)
	defer writer.Flush()

	buf := make([]byte, util.BUFFER_SIZE)
	count, err := io.CopyBuffer(writer, r, buf)

	ch.incomingChannel.Error = err
	ch.incomingChannel.Counter = count

	log.Printf("in memory %s finish writing %s bytes:%d %v", writerName, channelName, count, err)
}
