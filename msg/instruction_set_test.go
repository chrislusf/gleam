package msg

import (
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestEncodingDecoding(t *testing.T) {

	instructions := &InstructionSet{
		FlowHashCode: proto.Uint32(1234567),
		ReaderCount:  proto.Int32(23),
		Instructions: []*Instruction{
			&Instruction{
				Name: proto.String("map1"),
				Script: &Script{
					IsPipe: proto.Bool(true),
					Path:   proto.String("cat"),
					Args:   []string{"/etc/passwd"},
				},
			},
			&Instruction{
				Name: proto.String("map2"),
				Script: &Script{
					IsPipe: proto.Bool(true),
					Path:   proto.String("sort"),
				},
			},
			&Instruction{
				Name: proto.String("map4"),
				Script: &Script{
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
