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
	} else if i.GetInputSplitReader() != nil {
		i.GetInputSplitReader().InputShardLocation.setLocation(locations[0])
	} else if i.GetRoundRobin() != nil {
		i.GetRoundRobin().InputShardLocation.setLocation(locations[0])
	} else if i.GetLocalTop() != nil {
		i.GetLocalTop().InputShardLocation.setLocation(locations[0])
	} else if i.GetBroadcast() != nil {
		i.GetBroadcast().InputShardLocation.setLocation(locations[0])
	} else if i.GetLocalHashAndJoinWith() != nil {
		i.GetLocalHashAndJoinWith().LeftInputShardLocation.setLocation(locations[0])
		i.GetLocalHashAndJoinWith().RightInputShardLocation.setLocation(locations[1])
	} else {
		panic("need to set input locations for new instruction.")
	}
}

func (i *Instruction) SetOutputLocation(location resource.Location) {
	if i.GetScript() != nil {
		i.GetScript().OutputShardLocation.setLocation(location)
	} else if i.GetLocalSort() != nil {
		i.GetLocalSort().OutputShardLocation.setLocation(location)
	} else if i.GetPipeAsArgs() != nil {
		i.GetPipeAsArgs().OutputShardLocation.setLocation(location)
	} else if i.GetMergeSortedTo() != nil {
		i.GetMergeSortedTo().OutputShardLocation.setLocation(location)
	} else if i.GetScatterPartitions() != nil {
		for _, outputLocation := range i.GetScatterPartitions().GetOutputShardLocations() {
			outputLocation.setLocation(location)
		}
	} else if i.GetCollectPartitions() != nil {
		i.GetCollectPartitions().OutputShardLocation.setLocation(location)
	} else if i.GetJoinPartitionedSorted() != nil {
		i.GetJoinPartitionedSorted().OutputShardLocation.setLocation(location)
	} else if i.GetCoGroupPartitionedSorted() != nil {
		i.GetCoGroupPartitionedSorted().OutputShardLocation.setLocation(location)
	} else if i.GetInputSplitReader() != nil {
		i.GetInputSplitReader().OutputShardLocation.setLocation(location)
	} else if i.GetRoundRobin() != nil {
		for _, outputLocation := range i.GetRoundRobin().GetOutputShardLocations() {
			outputLocation.setLocation(location)
		}
	} else if i.GetLocalTop() != nil {
		i.GetLocalTop().OutputShardLocation.setLocation(location)
	} else if i.GetBroadcast() != nil {
		for _, outputLocation := range i.GetBroadcast().GetOutputShardLocations() {
			outputLocation.setLocation(location)
		}
	} else if i.GetLocalHashAndJoinWith() != nil {
		i.GetLocalHashAndJoinWith().OutputShardLocation.setLocation(location)
	} else {
		panic("need to set output locations for new instruction.")
	}
}

func (l *DatasetShardLocation) setLocation(loc resource.Location) {
	l.Host = proto.String(loc.Server)
	l.Port = proto.Int32(int32(loc.Port))
}
