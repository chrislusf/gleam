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

	log.Printf("in memory %s starts writing %s expected reader:%d", writerName, channelName, readerCount)

	writer := bufio.NewWriter(ch.incomingChannel.Writer)
	defer writer.Flush()

	buf := make([]byte, util.BUFFER_SIZE)
	count, err := io.CopyBuffer(writer, r, buf)

	ch.incomingChannel.Error = err
	ch.incomingChannel.Counter = count

	if err != nil {
		log.Printf("in memory %s finished writing %s %d bytes: %v", writerName, channelName, count, err)
	} else {
		log.Printf("in memory %s finished writing %s %d bytes", writerName, channelName, count)
	}
}
