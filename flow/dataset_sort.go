package flow

import (
	"bytes"
	"fmt"

	"github.com/psilva261/timsort"
	"github.com/ugorji/go/codec"
)

var (
	msgpackHandler codec.MsgpackHandle
)

type pair struct {
	key  []byte
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
		defer close(outChan)

		var kvs []interface{}
		for input := range task.InputShards[0].OutgoingChans[0] {
			var key []byte
			dec := codec.NewDecoderBytes(input, &msgpackHandler)
			if err := dec.Decode(&key); err != nil {
				fmt.Printf("Sort>Failed to read input data %v: %+v\n", err, input)
				break
			}
			kvs = append(kvs, pair{key: key, data: input})
		}
		if len(kvs) == 0 {
			return
		}
		timsort.Sort(kvs, func(a interface{}, b interface{}) bool {
			x, y := a.(pair), b.(pair)
			return bytes.Compare(x.key, y.key) < 0
		})

		for _, kv := range kvs {
			outChan <- kv.(pair).data
		}
	}
	return ret
}
