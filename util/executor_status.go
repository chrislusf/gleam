package util

import (
	"time"
)

type ChannelStatus struct {
	Length    int64
	StartTime time.Time
	StopTime  time.Time
	Name      string
}

func NewChannelStatus() *ChannelStatus {
	return &ChannelStatus{}
}

func (s *ChannelStatus) ReportStart() {
	s.StartTime = time.Now()
}

func (s *ChannelStatus) ReportAdd(delta int) {
	s.Length += int64(delta)
}

func (s *ChannelStatus) ReportClose() {
	s.StopTime = time.Now()
}

type ExecutorStatus struct {
	InputChannelStatuses  []*ChannelStatus
	OutputChannelStatuses []*ChannelStatus
	RequestTime           time.Time
	StartTime             time.Time
	StopTime              time.Time
}

func (s *ExecutorStatus) IsClosed() bool {
	return !s.StopTime.IsZero()
}

func (s *ExecutorStatus) TimeTaken() time.Duration {
	if s.IsClosed() {
		return s.StopTime.Sub(s.RequestTime)
	}
	return time.Now().Sub(s.RequestTime)
}
