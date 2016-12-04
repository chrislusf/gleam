package instruction

import (
	"io"

	"github.com/chrislusf/gleam/msg"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	InstructionRunner.Register(func(m *msg.Instruction) Instruction {
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

func (b *JoinPartitionedSorted) Name() string {
	return "JoinPartitionedSorted"
}

func (b *JoinPartitionedSorted) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) error {
		return DoJoinPartitionedSorted(readers[0], readers[1], writers[0], b.indexes, b.isLeftOuterJoin, b.isRightOuterJoin)
	}
}

func (b *JoinPartitionedSorted) SerializeToCommand() *msg.Instruction {
	return &msg.Instruction{
		Name: proto.String(b.Name()),
		JoinPartitionedSorted: &msg.JoinPartitionedSorted{
			IsLeftOuterJoin:  proto.Bool(b.isLeftOuterJoin),
			IsRightOuterJoin: proto.Bool(b.isRightOuterJoin),
			Indexes:          getIndexes(b.indexes),
		},
	}
}

func (b *JoinPartitionedSorted) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

// Top streamingly compare and get the top n items
func DoJoinPartitionedSorted(leftRawChan, rightRawChan io.Reader, writer io.Writer, indexes []int,
	isLeftOuterJoin, isRightOuterJoin bool) error {
	leftChan := newChannelOfValuesWithSameKey("left", leftRawChan, indexes)
	rightChan := newChannelOfValuesWithSameKey("right", rightRawChan, indexes)

	// get first value from both channels
	leftValuesWithSameKey, leftHasValue := <-leftChan
	rightValuesWithSameKey, rightHasValue := <-rightChan

	var leftValueLength, rightValueLength int
	if leftHasValue {
		leftValueLength = len(leftValuesWithSameKey.Values[0].([]interface{}))
	}
	if rightHasValue {
		rightValueLength = len(rightValuesWithSameKey.Values[0].([]interface{}))
	}

	for leftHasValue && rightHasValue {
		x := util.Compare(leftValuesWithSameKey.Keys, rightValuesWithSameKey.Keys)
		switch {
		case x == 0:
			// left and right cartician join
			for _, a := range leftValuesWithSameKey.Values {
				for _, b := range rightValuesWithSameKey.Values {
					t := leftValuesWithSameKey.Keys
					t = append(t, a.([]interface{})...)
					t = append(t, b.([]interface{})...)
					util.WriteRow(writer, t...)
				}
			}
			leftValuesWithSameKey, leftHasValue = <-leftChan
			rightValuesWithSameKey, rightHasValue = <-rightChan
		case x < 0:
			if isLeftOuterJoin {
				for _, leftValue := range leftValuesWithSameKey.Values {
					t := leftValuesWithSameKey.Keys
					t = append(t, leftValue.([]interface{})...)
					t = addNils(t, rightValueLength)
					util.WriteRow(writer, t...)
				}
			}
			leftValuesWithSameKey, leftHasValue = <-leftChan
		case x > 0:
			if isRightOuterJoin {
				for _, rightValue := range rightValuesWithSameKey.Values {
					t := rightValuesWithSameKey.Keys
					t = addNils(t, leftValueLength)
					t = append(t, rightValue.([]interface{})...)
					util.WriteRow(writer, t...)
				}
			}
			rightValuesWithSameKey, rightHasValue = <-rightChan
		}
	}
	if leftHasValue {
		if isLeftOuterJoin {
			for _, leftValue := range leftValuesWithSameKey.Values {
				t := leftValuesWithSameKey.Keys
				t = append(t, leftValue.([]interface{})...)
				t = addNils(t, rightValueLength)
				util.WriteRow(writer, t...)
			}
		}
	}
	for leftValuesWithSameKey = range leftChan {
		if isLeftOuterJoin {
			for _, leftValue := range leftValuesWithSameKey.Values {
				t := leftValuesWithSameKey.Keys
				t = append(t, leftValue.([]interface{})...)
				t = addNils(t, rightValueLength)
				util.WriteRow(writer, t...)
			}
		}
	}
	if rightHasValue {
		if isRightOuterJoin {
			for _, rightValue := range rightValuesWithSameKey.Values {
				t := rightValuesWithSameKey.Keys
				t = addNils(t, leftValueLength)
				t = append(t, rightValue.([]interface{})...)
				util.WriteRow(writer, t...)
			}
		}
	}
	for rightValuesWithSameKey = range rightChan {
		if isRightOuterJoin {
			for _, rightValue := range rightValuesWithSameKey.Values {
				t := rightValuesWithSameKey.Keys
				t = addNils(t, leftValueLength)
				t = append(t, rightValue.([]interface{})...)
				util.WriteRow(writer, t...)
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
