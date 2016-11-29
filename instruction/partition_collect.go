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
			return NewCollectPartitions(
				m.GetOnDisk(),
			)
		}
		return nil
	})
}

type CollectPartitions struct {
	onDisk bool
}

func NewCollectPartitions(onDisk bool) *CollectPartitions {
	return &CollectPartitions{onDisk}
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
		OnDisk:            proto.Bool(b.onDisk),
		CollectPartitions: &msg.CollectPartitions{},
	}
}

func (b *CollectPartitions) GetMemoryCostInMB(partitionSize int64) int64 {
	return 3
}

func DoCollectPartitions(readers []io.Reader, writer io.Writer) {
	// println("starting to collect data from partitions...", len(readers))

	if len(readers) == 1 {
		io.Copy(writer, readers[0])
		return
	}

	util.CopyMultipleReaders(readers, writer)
}
