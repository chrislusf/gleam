package flow

import (
	"log"
)

func NewDataset(context *FlowContext) *Dataset {
	d := &Dataset{
		FlowContext: context,
	}
	context.Datasets = append(context.Datasets, d)
	return d
}

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

func (d *Dataset) Script(scriptType string) *Dataset {
	if _, ok := d.FlowContext.Scripts[scriptType]; !ok {
		log.Fatalf("script type %s is not registered.", scriptType)
	}
	d.FlowContext.LastScriptType = scriptType
	return d
}
