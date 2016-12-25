package pb

import (
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestEncodingDecoding(t *testing.T) {

	instructions := &InstructionSet{
		FlowHashCode: 1234567,
		ReaderCount:  23,
		Instructions: []*Instruction{
			{
				Name: "map1",
				Script: &Script{
					IsPipe: true,
					Path:   "cat",
					Args:   []string{"/etc/passwd"},
				},
			},
			{
				Name: "map2",
				Script: &Script{
					IsPipe: true,
					Path:   "sort",
				},
			},
			{
				Name: "map4",
				Script: &Script{
					IsPipe: true,
					Path:   "cat",
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
