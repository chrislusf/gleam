package instruction

import (
	"io"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetRoundRobin() != nil {
			return NewRoundRobin()
		}
		return nil
	})
}

type RoundRobin struct {
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (b *RoundRobin) Name() string {
	return "RoundRobin"
}

func (b *RoundRobin) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoRoundRobin(readers[0], writers)
	}
}

func (b *RoundRobin) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name:       proto.String(b.Name()),
		RoundRobin: &msg.RoundRobin{},
	}
}

func (b *RoundRobin) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoRoundRobin(reader io.Reader, writers []io.Writer) error {
	count, shardCount := 0, len(writers)
	return util.ProcessMessage(reader, func(data []byte) error {
		if count >= shardCount {
			count = 0
		}
		util.WriteMessage(writers[count], data)
		count++
		return nil
	})
}
