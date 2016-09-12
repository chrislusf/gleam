package flow

import (
	"fmt"

	"github.com/chrislusf/gleam/util"
	"github.com/psilva261/timsort"
	"github.com/ugorji/go/codec"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type pair struct {
	key  interface{}
	data []byte
}

func (d *Dataset) LocalSort() *Dataset {
	if d.IsLocalSorted {
		return d
	}

	ret, step := add1ShardTo1Step(d)
	ret.IsLocalSorted = true
	step.Name = "LocalSort"
	step.NetworkType = OneShardToOneShard
	step.Function = func(task *Task) {
		outChan := task.OutputShards[0].IncomingChan

		var kvs []interface{}
		for input := range task.InputShards[0].OutgoingChans[0] {
			if key, err := util.DecodeRowKey(input); err != nil {
				fmt.Printf("Sort>Failed to read input data %v: %+v\n", err, input)
				break
			} else {
				kvs = append(kvs, pair{key: key, data: input})
			}
		}
		if len(kvs) == 0 {
			return
		}
		timsort.Sort(kvs, func(a, b interface{}) bool {
			x, y := a.(pair), b.(pair)
			return util.LessThan(x.key, y.key)
		})

		for _, kv := range kvs {
			outChan <- kv.(pair).data
		}

		for _, shard := range task.OutputShards {
			close(shard.IncomingChan)
		}
	}
	return ret
}
