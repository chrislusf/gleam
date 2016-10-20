package ints

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/chrislusf/gleam/source"
	"github.com/chrislusf/gleam/util"
)

func init() {
	source.Registry.Register("ints", &IntsInputFormat{})
}

type IntsInputFormat struct {
}

func (cs *IntsInputFormat) GetInputSplitter(input source.Input) source.InputSplitter {
	return &IntsInputSplitter{input.(*IntsInput)}
}

func (cs *IntsInputFormat) GetInputSplitReader(inputSplit source.InputSplit) (source.InputSplitReader, error) {
	return NewIntsSplitInputReader(inputSplit.(*IntsInputSplit))
}

func (cs *IntsInputFormat) EncodeInputSplit(inputSplit source.InputSplit) ([]byte, error) {
	var buf bytes.Buffer

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(inputSplit)
	if err != nil {
		return nil, fmt.Errorf("Encode %+v error: %v", inputSplit, err)
	}

	return buf.Bytes(), nil
}

func (cs *IntsInputFormat) DecodeInputSplit(serializedInputSplit []byte) (source.InputSplit, error) {
	buf := bytes.NewBuffer(serializedInputSplit)

	dec := gob.NewDecoder(buf)
	v := IntsInputSplit{}
	err := dec.Decode(&v)

	return &v, err
}

type IntsInputSplit struct {
	SplitNumber         int
	TotalNumberOfSplits int
	Start               int
	Stop                int
}

func (cis *IntsInputSplit) GetSplitNumber() int {
	return cis.SplitNumber
}

func (cis *IntsInputSplit) GetTotalNumberOfSplits() int {
	return cis.TotalNumberOfSplits
}

type IntsInputSplitter struct {
	input *IntsInput
}

func (cs *IntsInputSplitter) Split() (splits []source.InputSplit) {
	width := cs.input.Stop - cs.input.Start
	totalSplits := width / cs.input.SplitSize
	if width%cs.input.SplitSize > 0 {
		totalSplits += 1
	}

	start := cs.input.Start
	for index := 0; start < cs.input.Stop; index++ {
		stop := cs.input.Stop
		if start+cs.input.SplitSize < stop {
			stop = start + cs.input.SplitSize
		}
		splits = append(splits, &IntsInputSplit{
			SplitNumber:         index,
			TotalNumberOfSplits: totalSplits,
			Start:               start,
			Stop:                stop,
		})
		start += cs.input.SplitSize
	}
	return splits
}

type IntsSplitInputReader struct {
	split *IntsInputSplit
	next  int
}

func NewIntsSplitInputReader(split *IntsInputSplit) (*IntsSplitInputReader, error) {
	csir := &IntsSplitInputReader{
		split: split,
		next:  split.Start,
	}
	return csir, nil
}

func (ir *IntsSplitInputReader) WriteRow(writer io.Writer) bool {
	if ir.next >= ir.split.Stop {
		return false
	}
	util.WriteRow(writer, ir.next)
	ir.next++
	return true
}

func (ir *IntsSplitInputReader) Close() {
	ir.next = ir.split.Start
}
