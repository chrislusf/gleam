package flow

import (
	"github.com/chrislusf/gleam/util"
)

func (d *Dataset) CoGroup(other *Dataset) *Dataset {
	sorted_d := d.Partition(len(d.Shards)).LocalSort()
	if d == other {
		// this should not happen, but just in case
		return sorted_d.LocalGroupByKey()
	}
	sorted_other := other.Partition(len(d.Shards)).LocalSort()
	return sorted_d.CoGroupPartitionedSorted(sorted_other)
}

func (d *Dataset) LocalGroupByKey() *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "LocalGroupByKey"
	step.Script = d.FlowContext.CreateScript()
	step.Script.GroupByKey()
	return ret
}

// CoGroupPartitionedSorted joins 2 datasets that are sharded
// by the same key and already locally sorted within each shard.
func (this *Dataset) CoGroupPartitionedSorted(that *Dataset) (ret *Dataset) {
	ret = this.FlowContext.newNextDataset(len(this.Shards))

	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "CoGroupPartitionedSorted"
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan
		defer close(outChan)

		leftChan := newChannelOfValuesWithSameKey(task.InputShards[0].OutgoingChans[0])
		rightChan := newChannelOfValuesWithSameKey(task.InputShards[1].OutgoingChans[0])

		// get first value from both channels
		leftValuesWithSameKey, leftHasValue := <-leftChan
		rightValuesWithSameKey, rightHasValue := <-rightChan

		for leftHasValue && rightHasValue {
			x := util.Compare(leftValuesWithSameKey.Key, rightValuesWithSameKey.Key)
			switch {
			case x == 0:
				util.WriteRow(outChan, leftValuesWithSameKey.Key, leftValuesWithSameKey.Values, rightValuesWithSameKey.Values)
				leftValuesWithSameKey, leftHasValue = <-leftChan
				rightValuesWithSameKey, rightHasValue = <-rightChan
			case x < 0:
				util.WriteRow(outChan, leftValuesWithSameKey.Key, leftValuesWithSameKey.Values, []interface{}{})
				leftValuesWithSameKey, leftHasValue = <-leftChan
			case x > 0:
				util.WriteRow(outChan, rightValuesWithSameKey.Key, []interface{}{}, rightValuesWithSameKey.Values)
				rightValuesWithSameKey, rightHasValue = <-rightChan
			}
		}
		for leftHasValue {
			util.WriteRow(outChan, leftValuesWithSameKey.Key, leftValuesWithSameKey.Values, []interface{}{})
			leftValuesWithSameKey, leftHasValue = <-leftChan
		}
		for rightHasValue {
			util.WriteRow(outChan, rightValuesWithSameKey.Key, []interface{}{}, rightValuesWithSameKey.Values)
			rightValuesWithSameKey, rightHasValue = <-rightChan
		}
	}
	return ret
}
