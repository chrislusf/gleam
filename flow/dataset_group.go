package flow

func (d *Dataset) GroupBy(indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	orderBys := getOrderBysFromIndexes(indexes)
	ret := d.LocalSort(orderBys).LocalGroupBy(indexes)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, orderBys).LocalGroupBy(indexes)
	}
	ret.IsLocalSorted = orderBys
	return ret
}

func (d *Dataset) LocalGroupBy(indexes []int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsPartitionedBy = indexes
	step.Name = "LocalGroupBy"
	step.Script = d.FlowContext.CreateScript()
	step.Script.GroupBy(indexes)
	return ret
}
