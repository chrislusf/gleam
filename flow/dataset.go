package flow

import (
	"context"
	"fmt"
	"time"

	"github.com/chrislusf/gleam/util"
)

func newDataset(context *Flow) *Dataset {
	d := &Dataset{
		Id:   len(context.Datasets),
		Flow: context,
		Meta: &DasetsetMetadata{TotalSize: -1},
	}
	context.Datasets = append(context.Datasets, d)
	return d
}

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

func (d *Dataset) Script(scriptType string) *Dataset {
	d.Flow.Script(scriptType)
	return d
}

func (d *Dataset) Init(scriptPart string) *Dataset {
	d.Flow.Init(scriptPart)
	return d
}

// Run starts the whole flow. This is a convenient method, same as *Flow.Run()
func (d *Dataset) Run(option ...FlowOption) {
	d.RunContext(context.Background(), option...)
}

// Run starts the whole flow. This is a convenient method, same as *Flow.RunContext()
func (d *Dataset) RunContext(ctx context.Context, option ...FlowOption) {
	d.Flow.RunContext(ctx, option...)
}

func (d *Dataset) setupShard(n int) {
	for i := 0; i < n; i++ {
		ds := &DatasetShard{
			Id:           i,
			Dataset:      d,
			IncomingChan: util.NewPiper(),
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
	return fmt.Sprintf("f%d-d%d-s%d", s.Dataset.Flow.HashCode, s.Dataset.Id, s.Id)
}
