package flow

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/source"
	_ "github.com/chrislusf/gleam/source/csv"
	"github.com/chrislusf/gleam/util"
)

// Input specifies the input
// parallelCount specifies how many executors whould run in parallel to read the input
func (fc *FlowContext) Input(in source.Input, parallelCount int) (ret *Dataset) {
	format, err := source.Registry.GetInputFormat(in.GetType())
	if err != nil {
		log.Fatalf("input %s not supported: %v", in.GetType(), err)
	}
	splits := format.GetInputSplitter(in).Split()
	var encoded [][]byte
	for _, split := range splits {
		data, _ := format.EncodeInputSplit(split)
		encoded = append(encoded, data)
	}

	data := fc.Bytes(encoded).RoundRobin(parallelCount)

	ret = fc.newNextDataset(parallelCount)
	step := fc.AddOneToOneStep(data, ret)
	step.Name = "InputSplitReader"
	step.Params["inputType"] = in.GetType()
	step.FunctionType = TypeInputSplitReader
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan
		inChan := task.InputChans[0]

		ReadInputSplits(in.GetType(), inChan.Reader, outChan.Writer)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}
	}
	return

}

// ReadInputSplits runs on executors to read input splits
func ReadInputSplits(typeName string, reader io.Reader, writer io.Writer) {
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
			log.Fatalf("Failed to decode InputSplit %+v: %v", split, err)
		}

		for csvReader.WriteRow(writer) {
		}

		csvReader.Close()
	}

}
