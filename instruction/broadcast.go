package instruction

import (
	"io"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type Broadcast struct {
}

func NewBroadcast() *Broadcast {
	return &Broadcast{}
}

func (b *Broadcast) Name() string {
	return "Broadcast"
}

func (b *Broadcast) FunctionType() FunctionType {
	return TypeBroadcast
}

func (b *Broadcast) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoBroadcast(readers[0], writers)
	}
}

func (b *Broadcast) SerializeToCommand() *cmd.Instruction {
	return &cmd.Instruction{
		Name:      proto.String(b.Name()),
		Broadcast: &cmd.Broadcast{},
	}
}

func DoBroadcast(reader io.Reader, writers []io.Writer) {
	util.ProcessMessage(reader, func(data []byte) error {
		for _, writer := range writers {
			util.WriteMessage(writer, data)
		}
		return nil
	})
}
