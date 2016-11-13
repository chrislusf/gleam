package csv

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/util"
)

func (c *CsvAdapter) ReadSplit(split adapter.Split, writer io.Writer) error {
	ds, isCsvDataSplit := split.(CsvDataSplit)
	if !isCsvDataSplit {
		return fmt.Errorf("split is not CsvDataSplit? %v", split)
	}

	// println("opening file", ds.FileName)
	fr, err := filesystem.Open(ds.FileName)
	if err != nil {
		return fmt.Errorf("Failed to open file %s: %v", ds.FileName, err)
	}
	reader := NewReader(fr)
	if ds.HasHeader {
		reader.Read()
	}

	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		var oneRow []interface{}
		for _, field := range row {
			oneRow = append(oneRow, field)
		}
		util.WriteRow(writer, oneRow...)
	}

	fr.Close()

	return err
}
