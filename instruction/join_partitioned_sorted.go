package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(func(m *pb.Instruction) Instruction {
		if m.GetJoinPartitionedSorted() != nil {
			return NewJoinPartitionedSorted(
				m.GetJoinPartitionedSorted().GetIsLeftOuterJoin(),
				m.GetJoinPartitionedSorted().GetIsRightOuterJoin(),
				toInts(m.GetJoinPartitionedSorted().GetIndexes()),
			)
		}
		return nil
	})
}

type JoinPartitionedSorted struct {
	isLeftOuterJoin  bool
	isRightOuterJoin bool
	indexes          []int
}

func NewJoinPartitionedSorted(isLeftOuterJoin bool, isRightOuterJoin bool, indexes []int) *JoinPartitionedSorted {
	return &JoinPartitionedSorted{isLeftOuterJoin, isRightOuterJoin, indexes}
}

func (b *JoinPartitionedSorted) Name(prefix string) string {
	return prefix + ".JoinPartitionedSorted"
}

func (b *JoinPartitionedSorted) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoJoinPartitionedSorted(readers[0], readers[1], writers[0], b.indexes, b.isLeftOuterJoin, b.isRightOuterJoin, stats)
	}
}

func (b *JoinPartitionedSorted) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		JoinPartitionedSorted: &pb.Instruction_JoinPartitionedSorted{
			IsLeftOuterJoin:  (b.isLeftOuterJoin),
			IsRightOuterJoin: (b.isRightOuterJoin),
			Indexes:          getIndexes(b.indexes),
		},
	}
}

func (b *JoinPartitionedSorted) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

func DoJoinPartitionedSorted(leftRawChan, rightRawChan io.Reader, writer io.Writer, indexes []int,
	isLeftOuterJoin, isRightOuterJoin bool, stats *pb.InstructionStat) error {
	leftChan := newChannelOfValuesWithSameKey("left", leftRawChan, indexes)
	rightChan := newChannelOfValuesWithSameKey("right", rightRawChan, indexes)

	// get first value from both channels
	leftValuesWithSameKey, leftHasValue := <-leftChan
	rightValuesWithSameKey, rightHasValue := <-rightChan

	var leftValueLength, rightValueLength int
	if leftHasValue {
		leftValueLength = len(leftValuesWithSameKey.V[0].([]interface{}))
	}
	if rightHasValue {
		rightValueLength = len(rightValuesWithSameKey.V[0].([]interface{}))
	}

	for leftHasValue && rightHasValue {
		x := util.Compare(leftValuesWithSameKey.K, rightValuesWithSameKey.K)
		ts := max(leftValuesWithSameKey.T, rightValuesWithSameKey.T)
		switch {
		case x == 0:
			// left and right cartician join
			for _, a := range leftValuesWithSameKey.V {
				for _, b := range rightValuesWithSameKey.V {
					util.NewRow(ts).AppendKey(
						leftValuesWithSameKey.K...).AppendValue(
						a.([]interface{})...).AppendValue(
						b.([]interface{})...).WriteTo(writer)
					stats.OutputCounter++
				}
			}
			leftValuesWithSameKey, leftHasValue = <-leftChan
			rightValuesWithSameKey, rightHasValue = <-rightChan
			stats.InputCounter += 2
		case x < 0:
			if isLeftOuterJoin {
				for _, leftValue := range leftValuesWithSameKey.V {
					var t []interface{}
					t = append(t, leftValue.([]interface{})...)
					t = addNils(t, rightValueLength)
					util.NewRow(ts).AppendKey(
						leftValuesWithSameKey.K...).AppendValue(
						t...).WriteTo(writer)
					stats.OutputCounter++
				}
			}
			leftValuesWithSameKey, leftHasValue = <-leftChan
			stats.InputCounter++
		case x > 0:
			if isRightOuterJoin {
				for _, rightValue := range rightValuesWithSameKey.V {
					var t []interface{}
					t = addNils(t, leftValueLength)
					t = append(t, rightValue.([]interface{})...)
					util.NewRow(ts).AppendKey(
						rightValuesWithSameKey.K...).AppendValue(
						t...).WriteTo(writer)
					stats.OutputCounter++
				}
			}
			rightValuesWithSameKey, rightHasValue = <-rightChan
			stats.InputCounter++
		}
	}
	if leftHasValue {
		if isLeftOuterJoin {
			for _, leftValue := range leftValuesWithSameKey.V {
				var t []interface{}
				t = append(t, leftValue.([]interface{})...)
				t = addNils(t, rightValueLength)
				util.NewRow(leftValuesWithSameKey.T).AppendKey(
					leftValuesWithSameKey.K...).AppendValue(
					t...).WriteTo(writer)
				stats.OutputCounter++
			}
		}
	}
	for leftValuesWithSameKey = range leftChan {
		stats.InputCounter++
		if isLeftOuterJoin {
			for _, leftValue := range leftValuesWithSameKey.V {
				var t []interface{}
				t = append(t, leftValue.([]interface{})...)
				t = addNils(t, rightValueLength)
				util.NewRow(leftValuesWithSameKey.T).AppendKey(
					leftValuesWithSameKey.K...).AppendValue(
					t...).WriteTo(writer)
				stats.OutputCounter++
			}
		}
	}
	if rightHasValue {
		if isRightOuterJoin {
			for _, rightValue := range rightValuesWithSameKey.V {
				var t []interface{}
				t = addNils(t, leftValueLength)
				t = append(t, rightValue.([]interface{})...)
				util.NewRow(rightValuesWithSameKey.T).AppendKey(
					rightValuesWithSameKey.K...).AppendValue(
					t...).WriteTo(writer)
				stats.OutputCounter++
			}
		}
	}
	for rightValuesWithSameKey = range rightChan {
		stats.InputCounter++
		if isRightOuterJoin {
			for _, rightValue := range rightValuesWithSameKey.V {
				var t []interface{}
				t = addNils(t, leftValueLength)
				t = append(t, rightValue.([]interface{})...)
				util.NewRow(rightValuesWithSameKey.T).AppendKey(
					rightValuesWithSameKey.K...).AppendValue(
					t...).WriteTo(writer)
				stats.OutputCounter++
			}
		}
	}

	return nil

}

func addNils(target []interface{}, nilCount int) []interface{} {
	for i := 0; i < nilCount; i++ {
		target = append(target, nil)
	}
	return target
}
