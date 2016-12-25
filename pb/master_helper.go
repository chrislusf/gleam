package pb

import (
	"fmt"
)

func (l *Location) URL() string {
	return fmt.Sprintf("%s:%d", l.Server, l.Port)
}

// the distance is a relative value, similar to network lantency
func (a *Location) Distance(b *Location) float64 {
	if a.DataCenter != b.DataCenter {
		return 1000
	}
	if a.Rack != b.Rack {
		return 100
	}
	if a.Server != b.Server {
		return 10
	}
	return 1
}

func (a ComputeResource) Minus(b ComputeResource) ComputeResource {
	return ComputeResource{
		CpuCount: a.GetCpuCount() - b.GetCpuCount(),
		CpuLevel: a.GetCpuLevel(),
		MemoryMb: a.GetMemoryMb() - b.GetMemoryMb(),
		GpuCount: a.GetGpuCount() - b.GetGpuCount(),
		GpuLevel: a.GetGpuLevel(),
		DiskMb:   a.GetDiskMb() - b.GetDiskMb(),
	}
}

func (a ComputeResource) Plus(b ComputeResource) ComputeResource {
	return ComputeResource{
		CpuCount: a.GetCpuCount() + b.GetCpuCount(),
		CpuLevel: b.GetCpuLevel(),
		MemoryMb: a.GetMemoryMb() + b.GetMemoryMb(),
		GpuCount: a.GetGpuCount() + b.GetGpuCount(),
		GpuLevel: b.GetGpuLevel(),
		DiskMb:   a.GetDiskMb() + b.GetDiskMb(),
	}
}

func (a ComputeResource) GreaterThanZero() bool {
	return a.CpuCount > 0 && a.MemoryMb > 0
}

func (a ComputeResource) IsZero() bool {
	return a.CpuCount == 0 && a.MemoryMb == 0
}

func (a ComputeResource) Covers(b ComputeResource) bool {
	return a.CpuCount >= b.CpuCount && a.MemoryMb >= b.MemoryMb
}
