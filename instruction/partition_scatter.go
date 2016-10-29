package instruction

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type ScatterPartitions struct {
	indexes []int
}

func NewScatterPartitions(indexes []int) *ScatterPartitions {
	return &ScatterPartitions{indexes}
}

func (b *ScatterPartitions) Name() string {
	return "ScatterPartitions"
}

func (b *ScatterPartitions) FunctionType() FunctionType {
	return TypeScatterPartitions
}

func (b *ScatterPartitions) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoScatterPartitions(readers[0], writers, b.indexes)
	}
}

func (b *ScatterPartitions) SerializeToCommand() *cmd.Instruction {
	return &cmd.Instruction{
		Name: proto.String(b.Name()),
		ScatterPartitions: &cmd.ScatterPartitions{
			Indexes: getIndexes(b.indexes),
		},
	}
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
