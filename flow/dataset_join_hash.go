package flow

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/chrislusf/gleam/util"
)

// HashJoin joins two datasets by putting the smaller dataset in memory on all
// executors and streams through the bigger dataset.
func (bigger *Dataset) HashJoin(smaller *Dataset, indexes ...int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}
	return smaller.Broadcast(len(bigger.Shards)).LocalHashAndJoinWith(bigger, indexes)
}

func (this *Dataset) LocalHashAndJoinWith(that *Dataset, indexes []int) *Dataset {
	if len(indexes) == 0 {
		indexes = []int{1}
	}

	ret := this.FlowContext.newNextDataset(len(that.Shards))
	inputs := []*Dataset{this, that}
	step := this.FlowContext.MergeDatasets1ShardTo1Step(inputs, ret)
	step.Name = "LocalHashAndJoinWith"
	step.Params["indexes"] = indexes
	step.FunctionType = TypeLocalHashAndJoinWith
	step.Function = func(readers []io.Reader, writers []io.Writer, task *Task) {
		LocalHashAndJoinWith(
			readers[0],
			readers[1],
			indexes,
			writers[0],
		)
	}
	return ret
}

func LocalHashAndJoinWith(leftReader, rightReader io.Reader, indexes []int, outChan io.Writer) {
	hashmap := make(map[string][]interface{})
	err := util.ProcessMessage(leftReader, func(input []byte) error {
		if keys, vals, err := genKeyBytesAndValues(input, indexes); err != nil {
			return fmt.Errorf("%v: %+v", err, input)
		} else {
			hashmap[string(keys)] = vals
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Sort>Failed to read input data:%v\n", err)
	}
	if len(hashmap) == 0 {
		io.Copy(ioutil.Discard, rightReader)
		return
	}

	err = util.ProcessMessage(rightReader, func(input []byte) error {
		if keys, vals, err := util.DecodeRowKeysValues(input, indexes); err != nil {
			return fmt.Errorf("%v: %+v", err, input)
		} else {
			keyBytes, err := util.EncodeRow(keys...)
			if err != nil {
				return fmt.Errorf("Failed to encoded row %+v: %v", keys, err)
			}
			if mappedValues, ok := hashmap[string(keyBytes)]; ok {
				row := append(keys, vals...)
				row = append(row, mappedValues...)
				util.WriteRow(outChan, row...)
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("LocalHashAndJoinWith>Failed to process the bigger input data:%v\n", err)
	}
}

func genKeyBytesAndValues(input []byte, indexes []int) (keyBytes []byte, values []interface{}, err error) {
	keys, values, err := util.DecodeRowKeysValues(input, indexes)
	if err != nil {
		return nil, nil, fmt.Errorf("DecodeRowKeysValues %v: %+v", err, input)
	}
	keyBytes, err = util.EncodeRow(keys...)
	return keyBytes, values, err
}

// Broadcast replicates itself in all shards.
func (d *Dataset) Broadcast(shardCount int) *Dataset {
	if shardCount == 1 && len(d.Shards) == shardCount {
		return d
	}
	ret := d.FlowContext.newNextDataset(shardCount)
	step := d.FlowContext.AddOneToAllStep(d, ret)
	step.Name = "Broadcast"
	step.FunctionType = TypeBroadcast
	step.Function = func(readers []io.Reader, writers []io.Writer, task *Task) {
		Broadcast(readers[0], writers)
	}
	return ret
}

func Broadcast(inChan io.Reader, outChans []io.Writer) {
	util.ProcessMessage(inChan, func(data []byte) error {
		for _, outChan := range outChans {
			util.WriteMessage(outChan, data)
		}
		return nil
	})
}
