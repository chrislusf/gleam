package flow

type FlowContextOption func(c *FlowContextConfig)

type FlowContextConfig struct {
	OnDisk bool
}

func (d *FlowContext) Hint(options ...FlowContextOption) {
	var config FlowContextConfig
	for _, option := range options {
		option(&config)
	}
}

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

func (d *Dataset) GetPartitionSize() int64 {
	return d.GetTotalSize() / int64(len(d.Shards))
}

func (d *Dataset) GetIsOnDiskIO() bool {
	if d.Meta.OnDisk == ModeUnset {
		var isOnDisk bool
		for _, ds := range d.Step.InputDatasets {
			isOnDisk = isOnDisk || ds.GetIsOnDiskIO()
		}
		if isOnDisk {
			d.Meta.OnDisk = ModeOnDisk
		} else {
			d.Meta.OnDisk = ModeInMemory
		}
	}

	return d.Meta.OnDisk == ModeOnDisk
}
