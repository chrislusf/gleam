package instruction

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetScatterPartitions() != nil {
			return NewScatterPartitions(
				toInts(m.GetScatterPartitions().GetIndexes()),
			)
		}
		return nil
	})
}

type ScatterPartitions struct {
	indexes []int
}

func NewScatterPartitions(indexes []int) *ScatterPartitions {
	return &ScatterPartitions{indexes}
}

func (b *ScatterPartitions) Name() string {
	return "ScatterPartitions"
}

func (b *ScatterPartitions) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoScatterPartitions(readers[0], writers, b.indexes)
	}
}

func (b *ScatterPartitions) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		ScatterPartitions: &msg.ScatterPartitions{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *ScatterPartitions) GetMemoryCostInMB() int {
	return 1
}

func DoScatterPartitions(reader io.Reader, writers []io.Writer, indexes []int) {
	shardCount := len(writers)

	util.ProcessMessage(reader, func(data []byte) error {
		keyObjects, err := util.DecodeRowKeys(data, indexes)
		if err != nil {
			log.Printf("Failed to find keys on %v", indexes)
			return err
		}
		x := util.PartitionByKeys(shardCount, keyObjects)
		util.WriteMessage(writers[x], data)
		return nil
	})
}
