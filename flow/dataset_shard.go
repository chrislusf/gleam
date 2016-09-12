package flow

import (
	"time"
)

func (d *Dataset) SetupShard(n int) {
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Id:           i,
			Dataset:      d,
			IncomingChan: make(chan []byte, 64), // a buffered chan!
		}
		d.Shards = append(d.Shards, ds)
	}
}

func (s *DatasetShard) Closed() bool {
	return !s.CloseTime.IsZero()
}

func (s *DatasetShard) TimeTaken() time.Duration {
	if s.Closed() {
		return s.CloseTime.Sub(s.ReadyTime)
	}
	return time.Now().Sub(s.ReadyTime)
}
