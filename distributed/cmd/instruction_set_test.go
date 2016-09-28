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
		Port: proto.Int32(45326),
	}
	instructions := &InstructionSet{
		FlowHashCode: proto.Uint32(1234567),
		Instructions: []*Instruction{
			&Instruction{
				Script: &Script{
					InputShardLocation:  oneDatasetShardLocation,
					OutputShardLocation: oneDatasetShardLocation,
					Name:                proto.String("map1"),
					IsPipe:              proto.Bool(true),
					Path:                proto.String("cat"),
					Args:                []string{"/etc/passwd"},
				},
			},
			&Instruction{
				Script: &Script{
					InputShardLocation:  oneDatasetShardLocation,
					OutputShardLocation: oneDatasetShardLocation,
					Name:                proto.String("map2"),
					IsPipe:              proto.Bool(true),
					Path:                proto.String("sort"),
				},
			},
			&Instruction{
				Script: &Script{
					InputShardLocation:  oneDatasetShardLocation,
					OutputShardLocation: oneDatasetShardLocation,
					Name:                proto.String("map4"),
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
