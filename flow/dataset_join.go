package flow

import (
	"io"

	"github.com/chrislusf/gleam/util"
)

// Join joins two datasets by the key.
func (d *Dataset) Join(other *Dataset, indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	sorted_d := d.Partition(len(d.Shards), indexes...).LocalSort(indexes)
	var sorted_other *Dataset
	if d == other {
		sorted_other = sorted_d
	} else {
		sorted_other = other.Partition(len(d.Shards), indexes...).LocalSort(indexes)
	}
	return sorted_d.JoinPartitionedSorted(sorted_other, indexes, false, false)
}

// Join multiple datasets that are sharded by the same key, and locally sorted within the shard
func (this *Dataset) JoinPartitionedSorted(that *Dataset, indexes []int,
	isLeftOuterJoin, isRightOuterJoin bool) *Dataset {
	ret := this.FlowContext.newNextDataset(len(this.Shards))

	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "JoinPartitionedSorted"
	step.Params["indexes"] = indexes
	step.FunctionType = TypeJoinPartitionedSorted
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		leftReader := task.InputChans[0].Reader
		rightReader := task.InputChans[1].Reader
		/*
			if leftReader == rightReader {
				// special case for self join
				rightReader = task.InputShards[0].OutgoingChans[1].Reader
			}
		*/
		JoinPartitionedSorted(
			leftReader,
			rightReader,
			indexes,
			isLeftOuterJoin,
			isRightOuterJoin,
			outChan.Writer,
		)

		for _, shard := range task.OutputShards {
			shard.IncomingChan.Writer.Close()
		}
	}
	return ret
}

func JoinPartitionedSorted(leftRawChan, rightRawChan io.Reader, indexes []int,
	isLeftOuterJoin, isRightOuterJoin bool, outChan io.Writer) {
	leftChan := newChannelOfValuesWithSameKey(leftRawChan, indexes)
	rightChan := newChannelOfValuesWithSameKey(rightRawChan, indexes)

	// get first value from both channels
	leftValuesWithSameKey, leftHasValue := <-leftChan
	rightValuesWithSameKey, rightHasValue := <-rightChan

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
					util.WriteRow(outChan, t...)
				}
			}
			leftValuesWithSameKey, leftHasValue = <-leftChan
			rightValuesWithSameKey, rightHasValue = <-rightChan
		case x < 0:
			if isLeftOuterJoin {
				for _, leftValue := range leftValuesWithSameKey.Values {
					t := leftValuesWithSameKey.Keys
					t = append(t, leftValue.([]interface{})...)
					util.WriteRow(outChan, t...)
				}
			}
			leftValuesWithSameKey, leftHasValue = <-leftChan
		case x > 0:
			if isRightOuterJoin {
				for _, rightValue := range rightValuesWithSameKey.Values {
					t := rightValuesWithSameKey.Keys
					t = append(t, rightValue.([]interface{})...)
					util.WriteRow(outChan, t...)
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
				util.WriteRow(outChan, t...)
			}
		}
	}
	for leftValuesWithSameKey = range leftChan {
		if isLeftOuterJoin {
			for _, leftValue := range leftValuesWithSameKey.Values {
				t := leftValuesWithSameKey.Keys
				t = append(t, leftValue.([]interface{})...)
				util.WriteRow(outChan, t...)
			}
		}
	}
	if rightHasValue {
		if isRightOuterJoin {
			for _, rightValue := range rightValuesWithSameKey.Values {
				t := rightValuesWithSameKey.Keys
				t = append(t, rightValue.([]interface{})...)
				util.WriteRow(outChan, t...)
			}
		}
	}
	for rightValuesWithSameKey = range rightChan {
		if isRightOuterJoin {
			for _, rightValue := range rightValuesWithSameKey.Values {
				t := rightValuesWithSameKey.Keys
				t = append(t, rightValue.([]interface{})...)
				util.WriteRow(outChan, t...)
			}
		}
	}

}
