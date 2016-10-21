package csv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"

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
	index := 0
	for _, fileName := range cs.input.FileNames {
		if !source.IsDir(fileName) {
			splits = append(splits, &CsvInputSplit{
				SplitNumber:         index,
				TotalNumberOfSplits: 0,
				FileName:            fileName,
				HasHeader:           cs.input.HasHeader,
			})
			index++
		} else {
			virtualFiles, err := source.List(fileName)
			if err != nil {
				log.Printf("Failed to list folder %s: %v", fileName, err)
				continue
			}
			for _, vf := range virtualFiles {
				if cs.input.Match(vf.Location) {
					splits = append(splits, &CsvInputSplit{
						SplitNumber:         index,
						TotalNumberOfSplits: 0,
						FileName:            vf.Location,
						HasHeader:           cs.input.HasHeader,
					})
					index++
				}
			}
		}
	}

	for _, split := range splits {
		s := split.(*CsvInputSplit)
		s.TotalNumberOfSplits = index
	}

	return splits
}

type CsvSplitInputReader struct {
	fileReader source.VirtualFile
	reader     *Reader
}

func NewCsvSplitInputReader(split *CsvInputSplit) (*CsvSplitInputReader, error) {
	fr, err := source.Open(split.FileName)
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
