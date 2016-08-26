package flow

import (
	"time"
)

func (d *Dataset) SetupShard(n int) {
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Dataset:      d,
			IncomingChan: make(chan []byte, 64), // a buffered chan!
		}
		d.Shards = append(d.Shards, ds)
	}
}

func (s *DatasetShard) SendForRead(t []byte) {
	for _, c := range s.OutgoingChans {
		// println(s.Name(), "send chan", i, "entry:", s.counter)
		c <- t
	}
}

func (s *DatasetShard) CloseRead() {
	for _, c := range s.OutgoingChans {
		close(c)
	}
	s.CloseTime = time.Now()
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
