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

func (b *CoGroupPartitionedSorted) Name(prefix string) string {
	return prefix + ".CoGroupPartitionedSorted"
}

func (b *CoGroupPartitionedSorted) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoCoGroupPartitionedSorted(readers[0], readers[1], writers[0], b.indexes, stats)
	}
}

func (b *CoGroupPartitionedSorted) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		CoGroupPartitionedSorted: &pb.Instruction_CoGroupPartitionedSorted{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *CoGroupPartitionedSorted) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

func DoCoGroupPartitionedSorted(leftRawChan, rightRawChan io.Reader, writer io.Writer, indexes []int, stats *pb.InstructionStat) error {
	leftChan := newChannelOfValuesWithSameKey("left", leftRawChan, indexes)
	rightChan := newChannelOfValuesWithSameKey("right", rightRawChan, indexes)

	// get first value from both channels
	leftValuesWithSameKey, leftHasValue := <-leftChan
	rightValuesWithSameKey, rightHasValue := <-rightChan

	for leftHasValue && rightHasValue {
		x := util.Compare(leftValuesWithSameKey.K, rightValuesWithSameKey.K)
		ts := max(leftValuesWithSameKey.T, rightValuesWithSameKey.T)
		switch {
		case x == 0:
			util.NewRow(ts).AppendKey(
				leftValuesWithSameKey.K...).AppendValue(
				leftValuesWithSameKey.V).AppendValue(
				rightValuesWithSameKey.V).WriteTo(writer)
			stats.OutputCounter++
			leftValuesWithSameKey, leftHasValue = <-leftChan
			rightValuesWithSameKey, rightHasValue = <-rightChan
			stats.InputCounter += 2
		case x < 0:
			util.NewRow(ts).AppendKey(
				leftValuesWithSameKey.K...).AppendValue(
				leftValuesWithSameKey.V).AppendValue(
				[]interface{}{}).WriteTo(writer)
			stats.OutputCounter++
			leftValuesWithSameKey, leftHasValue = <-leftChan
			stats.InputCounter++
		case x > 0:
			util.NewRow(ts).AppendKey(
				leftValuesWithSameKey.K...).AppendValue(
				[]interface{}{}).AppendValue(
				rightValuesWithSameKey.V).WriteTo(writer)
			stats.OutputCounter++
			rightValuesWithSameKey, rightHasValue = <-rightChan
			stats.InputCounter++
		}
	}
	for leftHasValue {
		util.NewRow(leftValuesWithSameKey.T).AppendKey(
			leftValuesWithSameKey.K...).AppendValue(
			leftValuesWithSameKey.V).AppendValue(
			[]interface{}{}).WriteTo(writer)
		stats.OutputCounter++
		leftValuesWithSameKey, leftHasValue = <-leftChan
		stats.InputCounter++
	}
	for rightHasValue {
		util.NewRow(rightValuesWithSameKey.T).AppendKey(
			rightValuesWithSameKey.K...).AppendValue(
			[]interface{}{}).AppendValue(
			rightValuesWithSameKey.V).WriteTo(writer)
		stats.OutputCounter++
		rightValuesWithSameKey, rightHasValue = <-rightChan
		stats.InputCounter++
	}
	return nil
}
