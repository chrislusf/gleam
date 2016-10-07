package cmd

import (
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestEncodingDecoding(t *testing.T) {

	oneDatasetShardLocation := &DatasetShardLocation{
		Shard: &DatasetShard{
			FlowName:       proto.String("test"),
			DatasetId:      proto.Int(1),
			DatasetShardId: proto.Int(0),
		},
		Host: proto.String("localhost"),
		Port: proto.Int32(45327),
	}
	instructions := &InstructionSet{
		FlowHashCode: proto.Uint32(1234567),
		ReaderCount:  proto.Int32(23),
		Instructions: []*Instruction{
			&Instruction{
				Name: proto.String("map1"),
				Script: &Script{
					InputShardLocation:  oneDatasetShardLocation,
					OutputShardLocation: oneDatasetShardLocation,
					IsPipe:              proto.Bool(true),
					Path:                proto.String("cat"),
					Args:                []string{"/etc/passwd"},
				},
			},
			&Instruction{
				Name: proto.String("map2"),
				Script: &Script{
					InputShardLocation:  oneDatasetShardLocation,
					OutputShardLocation: oneDatasetShardLocation,
					IsPipe:              proto.Bool(true),
					Path:                proto.String("sort"),
				},
			},
			&Instruction{
				Name: proto.String("map4"),
				Script: &Script{
					InputShardLocation:  oneDatasetShardLocation,
					OutputShardLocation: oneDatasetShardLocation,
					IsPipe:              proto.Bool(true),
					Path:                proto.String("cat"),
				},
			},
		},
	}

	data, err := proto.Marshal(instructions)
	if err != nil {
		t.Fatal("unmarshaling error: ", err)
	}

	newCmd := &InstructionSet{}
	if err := proto.Unmarshal(data, newCmd); err != nil {
		t.Fatal("unmarshaling error: ", err)
	}

}
