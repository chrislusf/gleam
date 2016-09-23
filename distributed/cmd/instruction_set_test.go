package cmd

import (
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestEncodingDecoding(t *testing.T) {

	instructions := &InstructionSet{
		Instructions: []*Instruction{
			&Instruction{
				Script: &Script{
					InputShard: &DatasetShard{
						FlowName:       proto.String("test"),
						DatasetId:      proto.Int(1),
						DatasetShardId: proto.Int(0),
					},
					OutputShard: &DatasetShard{
						FlowName:       proto.String("test"),
						DatasetId:      proto.Int(2),
						DatasetShardId: proto.Int(0),
					},
					Name:   proto.String("map1"),
					IsPipe: proto.Bool(true),
					Path:   proto.String("cat"),
					Args:   []string{"/etc/passwd"},
				},
			},
			&Instruction{
				Script: &Script{
					InputShard: &DatasetShard{
						FlowName:       proto.String("test"),
						DatasetId:      proto.Int(2),
						DatasetShardId: proto.Int(0),
					},
					OutputShard: &DatasetShard{
						FlowName:       proto.String("test"),
						DatasetId:      proto.Int(3),
						DatasetShardId: proto.Int(0),
					},
					Name:   proto.String("map2"),
					IsPipe: proto.Bool(true),
					Path:   proto.String("sort"),
				},
			},
			&Instruction{
				Script: &Script{
					InputShard: &DatasetShard{
						FlowName:       proto.String("test"),
						DatasetId:      proto.Int(3),
						DatasetShardId: proto.Int(0),
					},
					OutputShard: &DatasetShard{
						FlowName:       proto.String("test"),
						DatasetId:      proto.Int(4),
						DatasetShardId: proto.Int(0),
					},
					Name:   proto.String("map4"),
					IsPipe: proto.Bool(true),
					Path:   proto.String("cat"),
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
