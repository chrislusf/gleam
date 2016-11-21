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
func TotalSize(n int) DasetsetHint {
	return func(d *Dataset) {
		d.Meta.TotalSize = n
	}
}

// PartitionSize hints the partition size in MB.
// This is usually used when sorting is needed.
func PartitionSize(n int) DasetsetHint {
	return func(d *Dataset) {
		d.Meta.TotalSize = n * len(d.GetShards())
	}
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
