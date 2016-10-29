package instruction

import (
	"io"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type RoundRobin struct {
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (b *RoundRobin) Name() string {
	return "RoundRobin"
}

func (b *RoundRobin) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoRoundRobin(readers[0], writers)
	}
}

func (b *RoundRobin) SerializeToCommand() *cmd.Instruction {
	return &cmd.Instruction{
		Name:       proto.String(b.Name()),
		RoundRobin: &cmd.RoundRobin{},
	}
}

func DoRoundRobin(reader io.Reader, writers []io.Writer) {
	count, shardCount := 0, len(writers)
	util.ProcessMessage(reader, func(data []byte) error {
		if count >= shardCount {
			count = 0
		}
		util.WriteMessage(writers[count], data)
		count++
		return nil
	})
}
