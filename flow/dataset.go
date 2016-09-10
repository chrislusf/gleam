package flow

import (
	"log"
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
