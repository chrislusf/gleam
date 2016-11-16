package instruction

import (
	"io"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetCollectPartitions() != nil {
			return NewCollectPartitions()
		}
		return nil
	})
}

type CollectPartitions struct {
}

func NewCollectPartitions() *CollectPartitions {
	return &CollectPartitions{}
}

func (b *CollectPartitions) Name() string {
	return "CollectPartitions"
}

func (b *CollectPartitions) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoCollectPartitions(readers, writers[0])
	}
}

func (b *CollectPartitions) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name:              proto.String(b.Name()),
		CollectPartitions: &msg.CollectPartitions{},
	}
}

func (b *CollectPartitions) GetMemoryCostInMB() int {
	return 1
}

func DoCollectPartitions(readers []io.Reader, writer io.Writer) {
	// println("starting to collect data from partitions...", len(readers))

	if len(readers) == 1 {
		io.Copy(writer, readers[0])
		return
	}

	util.CopyMultipleReaders(readers, writer)
}
