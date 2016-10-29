package instruction

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/source"
	_ "github.com/chrislusf/gleam/source/csv"
	_ "github.com/chrislusf/gleam/source/ints"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type InputSplitReader struct {
	typeName string
}

func NewInputSplitReader(typeName string) *InputSplitReader {
	return &InputSplitReader{typeName}
}

func (b *InputSplitReader) Name() string {
	return "InputSplitReader"
}

func (b *InputSplitReader) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoInputSplitReader(readers[0], writers[0], b.typeName)
	}
}

func (b *InputSplitReader) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		InputSplitReader: &msg.InputSplitReader{
			InputType: proto.String(b.typeName),
		},
	}
}

func DoInputSplitReader(reader io.Reader, writer io.Writer, typeName string) {
	format, err := source.Registry.GetInputFormat(typeName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load input format %s: %v", typeName, err)
		return
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
		split, err := format.DecodeInputSplit(encodedSplit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "decoding error: %v", err)
			continue
		}
		csvReader, err := format.GetInputSplitReader(split)
		if err != nil {
			log.Fatalf("Failed to read from InputSplit %+v: %v", split, err)
		}

		for csvReader.WriteRow(writer) {
		}

		csvReader.Close()
	}
}
