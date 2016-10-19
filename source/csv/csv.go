package csv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/source"
	"github.com/chrislusf/gleam/util"
)

func init() {
	source.Registry.Register("csv", &CsvInputFormat{})
}

type CsvInputFormat struct {
}

func (cs *CsvInputFormat) GetInputSplitter(input source.Input) source.InputSplitter {
	return &CsvInputSplitter{input.(*CsvInput)}
}

func (cs *CsvInputFormat) GetInputSplitReader(inputSplit source.InputSplit) (source.InputSplitReader, error) {
	return NewCsvSplitInputReader(inputSplit.(*CsvInputSplit))
}

func (cs *CsvInputFormat) EncodeInputSplit(inputSplit source.InputSplit) ([]byte, error) {
	var buf bytes.Buffer

	// Create an encoder and send a value.
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(inputSplit)
	if err != nil {
		return nil, fmt.Errorf("Encode %+v error: %v", inputSplit, err)
	}

	return buf.Bytes(), nil
}

func (cs *CsvInputFormat) DecodeInputSplit(serializedInputSplit []byte) (source.InputSplit, error) {
	buf := bytes.NewBuffer(serializedInputSplit)

	dec := gob.NewDecoder(buf)
	v := CsvInputSplit{}
	err := dec.Decode(&v)

	return &v, err
}

type CsvInput struct {
	FileNames []string
	HasHeader bool
}

func NewCsvInput(fileNames []string, hasHeader bool) *CsvInput {
	return &CsvInput{
		FileNames: fileNames,
		HasHeader: hasHeader,
	}
}

func (ci *CsvInput) GetType() string {
	return "csv"
}

type CsvInputSplit struct {
	SplitNumber         int
	TotalNumberOfSplits int
	FileName            string
	HasHeader           bool
}

func (cis *CsvInputSplit) GetSplitNumber() int {
	return cis.SplitNumber
}

func (cis *CsvInputSplit) GetTotalNumberOfSplits() int {
	return cis.TotalNumberOfSplits
}

type CsvInputSplitter struct {
	input *CsvInput
}

func (cs *CsvInputSplitter) Split() (splits []source.InputSplit) {
	for index, fileName := range cs.input.FileNames {
		splits = append(splits, &CsvInputSplit{
			SplitNumber:         index,
			TotalNumberOfSplits: len(cs.input.FileNames),
			FileName:            fileName,
			HasHeader:           cs.input.HasHeader,
		})
	}
	return splits
}

type CsvSplitInputReader struct {
	fileReader *os.File
	reader     *Reader
}

func NewCsvSplitInputReader(split *CsvInputSplit) (*CsvSplitInputReader, error) {
	fr, err := os.Open(split.FileName)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file %s: %v", split.FileName, err)
	}
	csir := &CsvSplitInputReader{
		fileReader: fr,
		reader:     NewReader(fr),
	}
	if split.HasHeader {
		csir.readRow()
	}
	return csir, nil
}

func (ir *CsvSplitInputReader) readRow() ([]string, error) {
	return ir.reader.Read()
}

func (ir *CsvSplitInputReader) WriteRow(writer io.Writer) bool {
	row, err := ir.reader.Read()
	if err == nil {
		var oneRow []interface{}
		for _, field := range row {
			oneRow = append(oneRow, field)
		}
		util.WriteRow(writer, oneRow...)
		return true
	}
	return false
}

func (ir *CsvSplitInputReader) Close() {
	ir.fileReader.Close()
}
