package instruction

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/chrislusf/gleam/distributed/cmd"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)

type LocalHashAndJoinWith struct {
	indexes []int
}

func NewLocalHashAndJoinWith(indexes []int) *LocalHashAndJoinWith {
	return &LocalHashAndJoinWith{indexes}
}

func (b *LocalHashAndJoinWith) Name() string {
	return "LocalHashAndJoinWith"
}

func (b *LocalHashAndJoinWith) FunctionType() FunctionType {
	return TypeLocalHashAndJoinWith
}

func (b *LocalHashAndJoinWith) Function() func(readers []io.Reader, writers []io.Writer, stats *Stats) {
	return func(readers []io.Reader, writers []io.Writer, stats *Stats) {
		DoLocalHashAndJoinWith(readers[0], readers[1], writers[0], b.indexes)
	}
}

func (b *LocalHashAndJoinWith) SerializeToCommand() *cmd.Instruction {
	return &cmd.Instruction{
		Name: proto.String(b.Name()),
		LocalHashAndJoinWith: &cmd.LocalHashAndJoinWith{
			Indexes: getIndexes(b.indexes),
		},
	}
}

// Top streamingly compare and get the top n items
func DoLocalHashAndJoinWith(leftReader, rightReader io.Reader, writer io.Writer, indexes []int) {
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
				util.WriteRow(writer, row...)
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
