package flow

import (
	"io"

	"github.com/chrislusf/gleam/util"
)

// CoGroup joins two datasets by the key,
// Each result row becomes this format:
//   (key, []left_rows, []right_rows)
func (d *Dataset) CoGroup(other *Dataset, indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	sorted_d := d.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	if d == other {
		// this should not happen, but just in case
		return sorted_d.LocalGroupBy(indexes)
	}
	sorted_other := other.Partition(len(d.Shards), indexes...).LocalSort(orderBys)
	return sorted_d.CoGroupPartitionedSorted(sorted_other, indexes)
}

// CoGroupPartitionedSorted joins 2 datasets that are sharded
// by the same key and already locally sorted within each shard.
func (this *Dataset) CoGroupPartitionedSorted(that *Dataset, indexes []int) (ret *Dataset) {
	ret = this.FlowContext.newNextDataset(len(this.Shards))

	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "CoGroupPartitionedSorted"
	step.Params["indexes"] = indexes
	step.FunctionType = TypeCoGroupPartitionedSorted
	step.Function = func(readers []io.Reader, writers []io.Writer, task *Task) {
		CoGroupPartitionedSorted(
			readers[0],
			readers[1],
			indexes,
			writers[0],
		)
	}
	return ret
}

func CoGroupPartitionedSorted(leftRawChan, rightRawChan io.Reader, indexes []int, writer io.Writer) {
	leftChan := newChannelOfValuesWithSameKey(leftRawChan, indexes)
	rightChan := newChannelOfValuesWithSameKey(rightRawChan, indexes)

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
