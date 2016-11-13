package flow

// Map operates on each row, and the returned results are passed to next dataset.
func (d *Dataset) Map(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Map"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Map(code)
	return ret
}

// ForEach operates on each row, but the results are not collected.
func (d *Dataset) ForEach(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "ForEach"
	step.Script = d.FlowContext.CreateScript()
	step.Script.ForEach(code)
	return ret
}

// FlatMap translates each row into multiple rows.
func (d *Dataset) FlatMap(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "FlatMap"
	step.Script = d.FlowContext.CreateScript()
	step.Script.FlatMap(code)
	return ret
}

// Filter conditionally filter some rows into the next dataset.
// The code should be a function just returning a boolean result.
func (d *Dataset) Filter(code string) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.Name = "Filter"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Filter(code)
	return ret
}

func add1ShardTo1Step(d *Dataset) (ret *Dataset, step *Step) {
	ret = d.FlowContext.newNextDataset(len(d.Shards))
	step = d.FlowContext.AddOneToOneStep(d, ret)
	return
}

// Select selects multiple fields into the next dataset. The index starts from 1.
func (d *Dataset) Select(indexes ...int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	step.Name = "Select"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Select(indexes)
	return ret
}

// LocalLimit take the local first n rows and skip all other rows.
func (d *Dataset) LocalLimit(n int) *Dataset {
	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = d.IsLocalSorted
	ret.IsPartitionedBy = d.IsPartitionedBy
	step.Name = "Limit"
	step.Script = d.FlowContext.CreateScript()
	step.Script.Limit(n)
	return ret
}
