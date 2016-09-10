package flow

import (
	"github.com/chrislusf/gleam/util"
)

// assume nothing about these two dataset
func (d *Dataset) Join(other *Dataset) *Dataset {
	sorted_d := d.Partition(len(d.Shards)).LocalSort()
	var sorted_other *Dataset
	if d == other {
		sorted_other = sorted_d
	} else {
		sorted_other = other.Partition(len(d.Shards)).LocalSort()
	}
	return sorted_d.JoinPartitionedSorted(sorted_other, false, false)
}

// Join multiple datasets that are sharded by the same key, and locally sorted within the shard
func (this *Dataset) JoinPartitionedSorted(that *Dataset,
	isLeftOuterJoin, isRightOuterJoin bool,
) (ret *Dataset) {
	ret = this.FlowContext.newNextDataset(len(this.Shards))

	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "JoinPartitionedSorted"
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		leftChan := newChannelOfValuesWithSameKey(task.InputShards[0].OutgoingChans[0])
		rightChan := newChannelOfValuesWithSameKey(task.InputShards[1].OutgoingChans[0])

		// get first value from both channels
		leftValuesWithSameKey, leftHasValue := <-leftChan
		rightValuesWithSameKey, rightHasValue := <-rightChan

		for leftHasValue && rightHasValue {
			x := util.Compare(leftValuesWithSameKey.Key, rightValuesWithSameKey.Key)
			switch {
			case x == 0:
				// left and right cartician join
				for _, a := range leftValuesWithSameKey.Values {
					for _, b := range rightValuesWithSameKey.Values {
						util.WriteRow(outChan, leftValuesWithSameKey.Key, a, b)
					}
				}
				leftValuesWithSameKey, leftHasValue = <-leftChan
				rightValuesWithSameKey, rightHasValue = <-rightChan
			case x < 0:
				if isLeftOuterJoin {
					for _, leftValue := range leftValuesWithSameKey.Values {
						util.WriteRow(outChan, leftValuesWithSameKey.Key, leftValue, nil)
					}
				}
				leftValuesWithSameKey, leftHasValue = <-leftChan
			case x > 0:
				if isRightOuterJoin {
					for _, rightValue := range rightValuesWithSameKey.Values {
						util.WriteRow(outChan, rightValuesWithSameKey.Key, nil, rightValue)
					}
				}
				rightValuesWithSameKey, rightHasValue = <-rightChan
			}
		}
		if leftHasValue {
			if isLeftOuterJoin {
				for _, leftValue := range leftValuesWithSameKey.Values {
					util.WriteRow(outChan, leftValuesWithSameKey.Key, leftValue, nil)
				}
			}
		}
		for leftValuesWithSameKey = range leftChan {
			if isLeftOuterJoin {
				for _, leftValue := range leftValuesWithSameKey.Values {
					util.WriteRow(outChan, leftValuesWithSameKey.Key, leftValue, nil)
				}
			}
		}
		if rightHasValue {
			if isRightOuterJoin {
				for _, rightValue := range rightValuesWithSameKey.Values {
					util.WriteRow(outChan, rightValuesWithSameKey.Key, nil, rightValue)
				}
			}
		}
		for rightValuesWithSameKey = range rightChan {
			if isRightOuterJoin {
				for _, rightValue := range rightValuesWithSameKey.Values {
					util.WriteRow(outChan, rightValuesWithSameKey.Key, nil, rightValue)
				}
			}
		}

	}
	return ret
}

type keyValues struct {
	Key    interface{}
	Values []interface{}
}

// create a channel to aggregate values of the same key
// automatically close original sorted channel
func newChannelOfValuesWithSameKey(sortedChan chan []byte) chan keyValues {
	outChan := make(chan keyValues)
	go func() {

		defer close(outChan)

		row, err := util.ReadRow(sortedChan)
		if err != nil {
			return
		}

		keyValues := keyValues{
			Key:    row[0],
			Values: []interface{}{row[1:]},
		}
		for {
			row, err = util.ReadRow(sortedChan)
			if err != nil {
				outChan <- keyValues
				break
			}
			x := util.Compare(keyValues.Key, row[0])
			if x == 0 {
				keyValues.Values = append(keyValues.Values, row[1:])
			} else {
				outChan <- keyValues
				keyValues.Key = row[0]
				keyValues.Values = []interface{}{row[1:]}
			}
		}
	}()

	return outChan
}
