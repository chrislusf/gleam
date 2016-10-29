package instruction

import (
	"io"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type CollectPartitions struct {
}

func NewCollectPartitions() *CollectPartitions {
	return &CollectPartitions{}
}

func (b *CollectPartitions) Name() string {
	return "CollectPartitions"
}

func (b *CollectPartitions) FunctionType() FunctionType {
	return TypeCollectPartitions
}

func (b *CollectPartitions) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoCollectPartitions(readers, writers[0])
	}
}

func (b *CollectPartitions) SerializeToCommand() *cmd.Instruction {
	return &cmd.Instruction{
		Name:              proto.String(b.Name()),
		CollectPartitions: &cmd.CollectPartitions{},
	}
}

func DoCollectPartitions(readers []io.Reader, writer io.Writer) {
	// println("starting to collect data from partitions...", len(readers))

	if len(readers) == 1 {
		io.Copy(writer, readers[0])
		return
	}

	util.CopyMultipleReaders(readers, writer)
}
