package msg

import (
	"fmt"

	"github.com/chrislusf/gleam/distributed/resource"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

func (m *DatasetShard) Name() string {
	return fmt.Sprintf("f%d-d%d-s%d", *m.FlowHashCode, *m.DatasetId, *m.DatasetShardId)
}

func (m *DatasetShardLocation) Address() string {
	return fmt.Sprintf("%s:%d", *m.Host, *m.Port)
}

func (m *InstructionSet) HashCode() uint32 {
	return util.Hash([]byte(m.String()))
}

func (m *InstructionSet) InstructionNames() (stepNames []string) {
	for _, ins := range m.GetInstructions() {
		stepNames = append(stepNames, ins.GetName())
	}
	return
}

func (i *Instruction) SetInputLocations(locations []resource.DataLocation) {
	for _, loc := range locations {
		i.InputShardLocations = append(i.InputShardLocations, &DatasetShardLocation{
			Name:   proto.String(loc.Name),
			Host:   proto.String(loc.Location.Server),
			Port:   proto.Int32(int32(loc.Location.Port)),
			OnDisk: proto.Bool(loc.OnDisk),
		})
	}
}

func (i *Instruction) SetOutputLocations(locations []resource.DataLocation) {
	for _, loc := range locations {
		i.OutputShardLocations = append(i.OutputShardLocations, &DatasetShardLocation{
			Name:   proto.String(loc.Name),
			Host:   proto.String(loc.Location.Server),
			Port:   proto.Int32(int32(loc.Location.Port)),
			OnDisk: proto.Bool(loc.OnDisk),
		})
	}
}
