package flow

// GroupBy e.g. GroupBy(Field(1,2,3)) group data by field 1,2,3
func (d *Dataset) GroupBy(sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret := d.LocalSort(sortOption).LocalGroupBy(sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption).LocalGroupBy(sortOption)
	}
	ret.IsLocalSorted = sortOption.orderByList
	return ret
}

func (d *Dataset) LocalGroupBy(sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret, step := add1ShardTo1Step(d)
	indexes := sortOption.Indexes()
	ret.IsPartitionedBy = indexes
	step.Name = "LocalGroupBy"
	step.Script = d.FlowContext.CreateScript()
	step.Script.GroupBy(indexes)
	return ret
}
