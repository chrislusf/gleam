package flow

func (d *Dataset) Reduce(code string) (ret *Dataset) {
	ret = d.LocalReduce(code)
	if len(d.Shards) > 1 {
		sortOption := Field(1)
		ret = ret.MergeSortedTo(1, sortOption).LocalReduce(code)
		ret.IsLocalSorted = sortOption.orderByList
	}
	return ret
}

func (d *Dataset) LocalReduce(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.Name = "LocalReduce"
	step.Script = d.FlowContext.createScript()
	step.Script.Reduce(code)
	return ret
}

func (d *Dataset) ReduceBy(code string, sortOptions ...*SortOption) (ret *Dataset) {
	sortOption := concat(sortOptions)

	ret = d.LocalSort(sortOption).LocalReduceBy(code, sortOption)
	if len(d.Shards) > 1 {
		ret = ret.MergeSortedTo(1, sortOption).LocalReduceBy(code, sortOption)
	}
	return ret
}

func (d *Dataset) LocalReduceBy(code string, sortOptions ...*SortOption) *Dataset {
	sortOption := concat(sortOptions)

	ret, step := add1ShardTo1Step(d)
	// TODO calculate IsLocalSorted IsPartitionedBy based on indexes
	step.Name = "LocalReduceBy"
	step.Script = d.FlowContext.createScript()
	step.Script.ReduceBy(code, sortOption.Indexes())
	return ret
}
