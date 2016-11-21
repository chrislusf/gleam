package instruction

import (
	"io"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetBroadcast() != nil {
			return NewBroadcast()
		}
		return nil
	})
}

type Broadcast struct {
}

func NewBroadcast() *Broadcast {
	return &Broadcast{}
}

func (b *Broadcast) Name() string {
	return "Broadcast"
}

func (b *Broadcast) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoBroadcast(readers[0], writers)
	}
}

func (b *Broadcast) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name:      proto.String(b.Name()),
		Broadcast: &msg.Broadcast{},
	}
}

func (b *Broadcast) GetMemoryCostInMB(partitionSize int) int {
	return 1
}

func DoBroadcast(reader io.Reader, writers []io.Writer) {
	util.ProcessMessage(reader, func(data []byte) error {
		for _, writer := range writers {
			util.WriteMessage(writer, data)
		}
		return nil
	})
}
