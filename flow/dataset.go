package flow

import (
	"log"
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
	if _, ok := d.FlowContext.Scripts[scriptType]; !ok {
		log.Fatalf("script type %s is not registered.", scriptType)
	}
	d.FlowContext.PrevScriptType = scriptType
	if len(scriptParts) > 0 {
		d.FlowContext.PrevScriptPart = scriptParts[0]
	}
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
