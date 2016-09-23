// Package resource manages computing resources and their locations.
package resource

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

type ComputeResource struct {
	CPUCount int   `json:"cpuCount,omitempty"`
	CPULevel int   `json:"cpuLevel,omitempty"` // higher number means higher compute power
	MemoryMB int64 `json:"memoryMB,omitempty"`
}

func (a ComputeResource) String() string {
	return fmt.Sprintf("CPUCount %d Level %d Memory %d MB", a.CPUCount, a.CPULevel, a.MemoryMB)
}

func (a ComputeResource) Minus(b ComputeResource) ComputeResource {
	return ComputeResource{
		CPUCount: a.CPUCount - b.CPUCount,
		MemoryMB: a.MemoryMB - b.MemoryMB,
	}
}

func (a ComputeResource) Plus(b ComputeResource) ComputeResource {
	return ComputeResource{
		CPUCount: a.CPUCount + b.CPUCount,
		MemoryMB: a.MemoryMB + b.MemoryMB,
	}
}

func (a ComputeResource) GreaterThanZero() bool {
	return a.CPUCount > 0 && a.MemoryMB > 0
}

func (a ComputeResource) IsZero() bool {
	return a.CPUCount == 0 && a.MemoryMB == 0
}

func (a ComputeResource) Covers(b ComputeResource) bool {
	return a.CPUCount >= b.CPUCount && a.MemoryMB >= b.MemoryMB
}

type ResourceOffer struct {
	ComputeResource
	ServerLocation Location
}

type DataResource struct {
	Location   Location `json:"location,omitempty"`
	DataSizeMB int      `json:"dataSizeMB,omitempty"`
}

type ComputeRequest struct {
	ComputeResource ComputeResource `json:"compute,omitempty"`
	Inputs          []DataResource  `json:"inputs,omitempty"`
}

type Location struct {
	DataCenter string `json:"dataCenter,omitempty"`
	Rack       string `json:"rack,omitempty"`
	Server     string `json:"server,omitempty"`
	Port       int    `json:"port,omitempty"`
}

type Allocation struct {
	Location  Location        `json:"location,omitempty"`
	Allocated ComputeResource `json:"allocated,omitempty"`
}

type AllocationRequest struct {
	Requests []ComputeRequest `json:"requests,omitempty"`
}

type AllocationResult struct {
	Allocations []Allocation `json:"allocations,omitempty"`
	Error       string       `json:"error,omitempty"`
}

func (l *Location) URL() string {
	return l.Server + ":" + strconv.Itoa(l.Port)
}

// the distance is a relative value, similar to network lantency
func (a Location) Distance(b Location) float64 {
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

func AddToValues(values url.Values, c *ComputeResource, allocated *ComputeResource) {
	values.Add("CPUCount", strconv.Itoa(c.CPUCount))
	values.Add("CPULevel", strconv.Itoa(c.CPULevel))
	values.Add("MemoryMB", strconv.FormatInt(c.MemoryMB, 10))
	values.Add("allocated.CPUCount", strconv.Itoa(allocated.CPUCount))
	values.Add("allocated.CPULevel", strconv.Itoa(allocated.CPULevel))
	values.Add("allocated.MemoryMB", strconv.FormatInt(allocated.MemoryMB, 10))
}

func NewComputeResourceFromRequest(r *http.Request) (ComputeResource, ComputeResource) {
	cpuCount, _ := strconv.ParseInt(r.FormValue("CPUCount"), 10, 32)
	cpuLevel, _ := strconv.ParseInt(r.FormValue("CPULevel"), 10, 32)
	memoryMB, _ := strconv.ParseInt(r.FormValue("MemoryMB"), 10, 64)
	availableCpuCount, _ := strconv.ParseInt(r.FormValue("allocated.CPUCount"), 10, 32)
	availableCpuLevel, _ := strconv.ParseInt(r.FormValue("allocated.CPULevel"), 10, 32)
	availableMemoryMB, _ := strconv.ParseInt(r.FormValue("allocated.MemoryMB"), 10, 64)
	return ComputeResource{
			CPUCount: int(cpuCount),
			CPULevel: int(cpuLevel),
			MemoryMB: memoryMB,
		}, ComputeResource{
			CPUCount: int(availableCpuCount),
			CPULevel: int(availableCpuLevel),
			MemoryMB: availableMemoryMB,
		}
}
