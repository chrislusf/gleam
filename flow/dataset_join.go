package flow

import (
	"io"

	"github.com/chrislusf/gleam/util"
)

// Join joins two datasets by the key.
func (d *Dataset) Join(other *Dataset, indexes ...int) *Dataset {
	return d.DoJoin(other, false, false, indexes)
}

func (d *Dataset) LeftOuterJoin(other *Dataset, indexes ...int) *Dataset {
	return d.DoJoin(other, true, false, indexes)
}

func (d *Dataset) RightOuterJoin(other *Dataset, indexes ...int) *Dataset {
	return d.DoJoin(other, false, true, indexes)
}

func (d *Dataset) DoJoin(other *Dataset, leftOuter, rightOuter bool, indexes []int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	sorted_d := d.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	var sorted_other *Dataset
	if d == other {
		sorted_other = sorted_d
	} else {
		sorted_other = other.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	}
	return sorted_d.JoinPartitionedSorted(sorted_other, indexes, leftOuter, rightOuter)
}

// JoinPartitionedSorted Join multiple datasets that are sharded by the same key, and locally sorted within the shard
func (this *Dataset) JoinPartitionedSorted(that *Dataset, indexes []int,
	isLeftOuterJoin, isRightOuterJoin bool) *Dataset {
	ret := this.FlowContext.newNextDataset(len(this.Shards))

	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "JoinPartitionedSorted"
	step.Params["indexes"] = indexes
	step.Params["isLeftOuterJoin"] = isLeftOuterJoin
	step.Params["isRightOuterJoin"] = isRightOuterJoin
	step.FunctionType = TypeJoinPartitionedSorted
	step.Function = func(readers []io.Reader, writers []io.Writer, task *Task) {
		JoinPartitionedSorted(
			readers[0],
			readers[1],
			indexes,
			isLeftOuterJoin,
			isRightOuterJoin,
			writers[0],
		)
	}
	return ret
}

func addNils(target []interface{}, nilCount int) []interface{} {
	for i := 0; i < nilCount; i++ {
		target = append(target, nil)
	}
	return target
}

func JoinPartitionedSorted(leftRawChan, rightRawChan io.Reader, indexes []int,
	isLeftOuterJoin, isRightOuterJoin bool, writer io.Writer) {
	leftChan := newChannelOfValuesWithSameKey(leftRawChan, indexes)
	rightChan := newChannelOfValuesWithSameKey(rightRawChan, indexes)

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

}
