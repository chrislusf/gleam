package flow

type DasetsetHint func(d *Dataset)

// Hint adds options for previous dataset.
func (d *Dataset) Hint(options ...DasetsetHint) *Dataset {
	for _, option := range options {
		option(d)
	}
	return d
}

// TotalSize hints the total size in MB for all the partitions.
// This is usually used when sorting is needed.
func TotalSize(n int64) DasetsetHint {
	return func(d *Dataset) {
		d.Meta.TotalSize = n
	}
}

// PartitionSize hints the partition size in MB.
// This is usually used when sorting is needed.
func PartitionSize(n int64) DasetsetHint {
	return func(d *Dataset) {
		d.Meta.TotalSize = n * int64(len(d.GetShards()))
	}
}

// OnDisk ensure the intermediate dataset are persisted to disk.
// This allows executors to run not in parallel if executors are limited.
func (d *Dataset) OnDisk(fn func(*Dataset) *Dataset) *Dataset {
	ret := fn(d)

	var parents, currents []*Dataset
	currents = append(currents, ret)
	for {
		for _, t := range currents {
			isFirstDataset, isLastDataset := false, false
			if t == ret {
				isLastDataset = true
			} else if t.Step.OutputDataset != nil {
				t.Step.OutputDataset.Meta.OnDisk = ModeOnDisk
			}
			for _, p := range t.Step.InputDatasets {
				if p != d {
					parents = append(parents, p)
				} else {
					isFirstDataset = true
				}
			}
			if !isFirstDataset && !isLastDataset {
				t.Step.Meta.IsRestartable = true
			}
		}
		if len(parents) == 0 {
			break
		}
		currents = parents
		parents = nil
	}

	return ret
}

/*

// Datacenter hints the previous dataset output location
func Datacenter(dc string) DasetsetOption {
	return func(c *DasetsetConfig) {
		c.Datacenter = dc
	}
}

// Rack hints the previous dataset output location
func Rack(rack string) DasetsetOption {
	return func(c *DasetsetConfig) {
		c.Rack = rack
	}
}

*/
