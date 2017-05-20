package flow

type FlowHintOption func(c *FlowConfig)

type FlowConfig struct {
	OnDisk bool
}

// Hint adds hints to the flow.
func (d *Flow) Hint(options ...FlowHintOption) {
	var config FlowConfig
	for _, option := range options {
		option(&config)
	}
}

// GetTotalSize returns the total size in MB for the dataset.
// This is based on the given hint.
func (d *Dataset) GetTotalSize() int64 {
	if d.Meta.TotalSize >= 0 {
		return d.Meta.TotalSize
	}
	var currentDatasetTotalSize int64
	for _, ds := range d.Step.InputDatasets {
		currentDatasetTotalSize += ds.GetTotalSize()
	}
	d.Meta.TotalSize = currentDatasetTotalSize
	return currentDatasetTotalSize
}

// GetPartitionSize returns the size in MB for each partition of
// the dataset. This is based on the hinted total size divided by
// the number of partitions.
func (d *Dataset) GetPartitionSize() int64 {
	return d.GetTotalSize() / int64(len(d.Shards))
}

// GetIsOnDiskIO returns true if the dataset is persisted
// to disk in distributed mode.
func (d *Dataset) GetIsOnDiskIO() bool {
	return d.Meta.OnDisk == ModeOnDisk
}
