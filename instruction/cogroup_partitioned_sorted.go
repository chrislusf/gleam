package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetCoGroupPartitionedSorted() != nil {
			return NewCoGroupPartitionedSorted(
				toInts(m.GetCoGroupPartitionedSorted().GetIndexes()),
			)
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

func (b *CoGroupPartitionedSorted) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoCoGroupPartitionedSorted(readers[0], readers[1], writers[0], b.indexes)
	}
}

func (b *CoGroupPartitionedSorted) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Name: b.Name(),
		CoGroupPartitionedSorted: &pb.Instruction_CoGroupPartitionedSorted{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *CoGroupPartitionedSorted) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

// Top streamingly compare and get the top n items
func DoCoGroupPartitionedSorted(leftRawChan, rightRawChan io.Reader, writer io.Writer, indexes []int) error {
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
	return nil
}
