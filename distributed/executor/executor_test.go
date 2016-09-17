package executor

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	proto "github.com/golang/protobuf/proto"
)

func TestExecutorScripts(t *testing.T) {

	outChan := make(chan []byte)

	executor := NewExecutor(nil, &cmd.InstructionSet{
		Instructions: []*cmd.Instruction{
			&cmd.Instruction{
				Script: &cmd.Script{
					InputShard:  &cmd.DatasetShard{},
					OutputShard: &cmd.DatasetShard{},
					Name:        proto.String("map1"),
					IsPipe:      proto.Bool(true),
					Path:        proto.String("cat"),
					Args:        []string{"/etc/passwd"},
				},
			},
			&cmd.Instruction{
				Script: &cmd.Script{
					InputShard:  &cmd.DatasetShard{},
					OutputShard: &cmd.DatasetShard{},
					Name:        proto.String("map2"),
					IsPipe:      proto.Bool(true),
					Path:        proto.String("sort"),
				},
			},
			&cmd.Instruction{
				Script: &cmd.Script{
					InputShard:  &cmd.DatasetShard{},
					OutputShard: &cmd.DatasetShard{},
					Name:        proto.String("map4"),
					IsPipe:      proto.Bool(true),
					Path:        proto.String("cat"),
				},
			},
		},
	})

	go executor.Execute(outChan)

	util.Fprintf(outChan, os.Stdout, "%s\n")

}
