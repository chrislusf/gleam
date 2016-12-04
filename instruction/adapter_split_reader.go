package instruction

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetAdapterSplitReader() != nil {
			return NewAdapterSplitReader(
				m.GetAdapterSplitReader().GetAdapterName(),
				m.GetAdapterSplitReader().GetConnectionId(),
			)
		}
		return nil
	})
}

type AdapterSplitReader struct {
	adapterName  string
	connectionId string
}

func NewAdapterSplitReader(adapterName, connectionId string) *AdapterSplitReader {
	return &AdapterSplitReader{adapterName, connectionId}
}

func (b *AdapterSplitReader) Name() string {
	return "AdapterSplitReader"
}

func (b *AdapterSplitReader) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoAdapterSplitReader(readers[0], writers[0], b.adapterName, b.connectionId)
	}
}

func (b *AdapterSplitReader) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		AdapterSplitReader: &msg.AdapterSplitReader{
			AdapterName:  proto.String(b.adapterName),
			ConnectionId: proto.String(b.adapterName),
		},
	}
}

func (b *AdapterSplitReader) GetMemoryCostInMB(partitionSize int64) int64 {
	return 3
}

func DoAdapterSplitReader(reader io.Reader, writer io.Writer, adapterName, connectionId string) error {
	a, found := adapter.AdapterManager.GetAdapter(adapterName)
	if !found {
		return fmt.Errorf("Failed to load adapter type %s", adapterName)
	}
	for {
		row, err := util.ReadRow(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "join read row error: %v", err)
			}
			break
		}
		encodedSplit := row[0].([]byte)

		split := decodeSplit(encodedSplit)

		a.LoadConfiguration(split.GetConfiguration())
		a.ReadSplit(split, writer)
	}
	return nil
}

func decodeSplit(data []byte) adapter.Split {
	network := bytes.NewBuffer(data)
	dec := gob.NewDecoder(network)
	return interfaceDecode(dec)
}

// interfaceDecode decodes the next interface value from the stream and returns it.
func interfaceDecode(dec *gob.Decoder) adapter.Split {
	var p adapter.Split
	err := dec.Decode(&p)
	if err != nil {
		log.Fatal("decode:", err)
	}
	return p
}
