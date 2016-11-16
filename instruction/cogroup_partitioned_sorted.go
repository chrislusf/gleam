package instruction

import (
	"io"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
		if m.GetCoGroupPartitionedSorted() != nil {
			return NewCoGroupPartitionedSorted(toInts(m.GetCoGroupPartitionedSorted().GetIndexes()))
		}
		return nil
	})
}

type CoGroupPartitionedSorted struct {
	indexes []int
}

func NewCoGroupPartitionedSorted(indexes []int) *CoGroupPartitionedSorted {
	return &CoGroupPartitionedSorted{indexes}
}

func (b *CoGroupPartitionedSorted) Name() string {
	return "CoGroupPartitionedSorted"
}

func (b *CoGroupPartitionedSorted) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoCoGroupPartitionedSorted(readers[0], readers[1], writers[0], b.indexes)
	}
}

func (b *CoGroupPartitionedSorted) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		CoGroupPartitionedSorted: &msg.CoGroupPartitionedSorted{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *CoGroupPartitionedSorted) GetMemoryCostInMB() int {
	return 1
}

// Top streamingly compare and get the top n items
func DoCoGroupPartitionedSorted(leftRawChan, rightRawChan io.Reader, writer io.Writer, indexes []int) {
	leftChan := newChannelOfValuesWithSameKey("left", leftRawChan, indexes)
	rightChan := newChannelOfValuesWithSameKey("right", rightRawChan, indexes)

	// get first value from both channels
	leftValuesWithSameKey, leftHasValue := <-leftChan
	rightValuesWithSameKey, rightHasValue := <-rightChan

	for leftHasValue && rightHasValue {
		x := util.Compare(leftValuesWithSameKey.Keys, rightValuesWithSameKey.Keys)
		switch {
		case x == 0:
			util.WriteRow(writer, leftValuesWithSameKey.Keys, leftValuesWithSameKey.Values, rightValuesWithSameKey.Values)
			leftValuesWithSameKey, leftHasValue = <-leftChan
			rightValuesWithSameKey, rightHasValue = <-rightChan
		case x < 0:
			util.WriteRow(writer, leftValuesWithSameKey.Keys, leftValuesWithSameKey.Values, []interface{}{})
			leftValuesWithSameKey, leftHasValue = <-leftChan
		case x > 0:
			util.WriteRow(writer, rightValuesWithSameKey.Keys, []interface{}{}, rightValuesWithSameKey.Values)
			rightValuesWithSameKey, rightHasValue = <-rightChan
		}
	}
	for leftHasValue {
		util.WriteRow(writer, leftValuesWithSameKey.Keys, leftValuesWithSameKey.Values, []interface{}{})
		leftValuesWithSameKey, leftHasValue = <-leftChan
	}
	for rightHasValue {
		util.WriteRow(writer, rightValuesWithSameKey.Keys, []interface{}{}, rightValuesWithSameKey.Values)
		rightValuesWithSameKey, rightHasValue = <-rightChan
	}
}
