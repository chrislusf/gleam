package flow

import (
	"log"

	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/source"
)

// Input specifies the input
// parallelCount specifies how many executors whould run in parallel to read the input
func (fc *FlowContext) Input(in source.Input) (ret *Dataset) {
	return fc.InputInParallel(in, -1)
}

// InputInParallel processes the input in parallel
// If parallelLimit <=0, the number of parallel processes will equal to the number of splits.
// Otherwise, the number of parallel processes equals to min(the number of splits, parallelLimit)
func (fc *FlowContext) InputInParallel(in source.Input, parallelLimit int) (ret *Dataset) {
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

	parallelCount := len(encoded)
	if parallelLimit > 0 && parallelCount > parallelLimit {
		parallelCount = parallelLimit
	}

	data := fc.Bytes(encoded).RoundRobin(parallelCount)

	ret = fc.newNextDataset(parallelCount)
	step := fc.AddOneToOneStep(data, ret)
	step.SetInstruction(instruction.NewInputSplitReader(in.GetType()))
	return

}
