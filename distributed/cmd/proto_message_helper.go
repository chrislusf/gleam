package cmd

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

func (i *Instruction) SetInputLocations(locations ...resource.Location) {
	if i.GetScript() != nil {
		i.GetScript().InputShardLocation.setLocation(locations[0])
	} else if i.GetLocalSort() != nil {
		i.GetLocalSort().InputShardLocation.setLocation(locations[0])
	} else if i.GetPipeAsArgs() != nil {
		i.GetPipeAsArgs().InputShardLocation.setLocation(locations[0])
	} else if i.GetMergeSortedTo() != nil {
		for index, inputLocation := range i.GetMergeSortedTo().GetInputShardLocations() {
			inputLocation.setLocation(locations[index])
		}
	} else if i.GetScatterPartitions() != nil {
		i.GetScatterPartitions().InputShardLocation.setLocation(locations[0])
	} else if i.GetCollectPartitions() != nil {
		for index, inputLocation := range i.GetCollectPartitions().GetInputShardLocations() {
			inputLocation.setLocation(locations[index])
		}
	} else if i.GetJoinPartitionedSorted() != nil {
		i.GetJoinPartitionedSorted().LeftInputShardLocation.setLocation(locations[0])
		i.GetJoinPartitionedSorted().RightInputShardLocation.setLocation(locations[1])
	} else if i.GetCoGroupPartitionedSorted() != nil {
		i.GetCoGroupPartitionedSorted().LeftInputShardLocation.setLocation(locations[0])
		i.GetCoGroupPartitionedSorted().RightInputShardLocation.setLocation(locations[1])
	}
}

func (l *DatasetShardLocation) setLocation(loc resource.Location) {
	l.Host = proto.String(loc.Server)
	l.Port = proto.Int32(int32(loc.Port))
}
