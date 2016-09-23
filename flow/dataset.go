package flow

import (
	"fmt"
	"time"
)

func NewDataset(context *FlowContext) *Dataset {
	d := &Dataset{
		Id:          len(context.Datasets),
		FlowContext: context,
	}
	context.Datasets = append(context.Datasets, d)
	return d
}

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

func (d *Dataset) Script(scriptType string, scriptParts ...string) *Dataset {
	d.FlowContext.Script(scriptType, scriptParts...)
	return d
}

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

func (s *DatasetShard) Name() string {
	return fmt.Sprintf("d%d-s%d", s.Dataset.Id, s.Id)
}
