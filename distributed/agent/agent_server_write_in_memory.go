package agent

import (
	"bufio"
	"io"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalInMemoryWriteConnection(r io.Reader, writerName, channelName string, readerCount int) {

	ch := as.inMemoryChannels.CreateNamedDatasetShard(channelName, readerCount)
	defer as.inMemoryChannels.Cleanup(channelName)
	defer ch.Writer.Close()

	// println(writerName, "start in memory writing to", channelName, "expected reader:", readerCount)

	reader := bufio.NewReaderSize(r, util.BUFFER_SIZE)
	writer := bufio.NewWriterSize(ch.Writer, util.BUFFER_SIZE)

	io.Copy(writer, reader)
	writer.Flush()

	// println(writerName, "finish writing to", channelName, count, "bytes")
}
