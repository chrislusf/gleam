package source

import (
	"io"
)

/*
How an input source work? Taking csv for example.

CsvInputFormat: a managerial type to manage CsvInputSplitter, CsvInputSplit, CsvInputSplitReader
CsvInput: any type
CsvInputSplitter: takes CsvInput, and produce a list of CsvInputSplit
CsvInputSplit: must be a serializable type, will be sent to executors
CsvInputSplitReader: runs on executors, read CsvInputSplit

When import this package:
  1. register mapping from "csv" to CsvInputFormat
On Driver:
  1. User specifiies CsvInput
  2. "csv"=>CsvInputFormat, +CsvInput => CsvSplitter.Split()/SplitBySize()/SplitByLines() => []CsvInputSplit
  3. send "csv", CsvInputSplit to each executor
On Executors:
  1. "csv"=>CsvInputFormat=>CsvInputFormat, CsvInputSplit, decode from []byte
  2. "csv"=>CsvInputFormat=>CsvInputSplitReader, read out lines by CsvInputSplit
*/

// a serializable spec of the input
// this would be sent to executors, and then retrieve and read the input data
// support a range of intergers
// support a list of files
// support a list of

// InputSplit must be a serializable object
type InputSplit interface {
	GetSplitNumber() int
	GetTotalNumberOfSplits() int
}

type InputSplitter interface {
	Split() []InputSplit
}

// RowIterator processes InputSplit, reads each row, and encode to msgpack format
type RowIterator interface {
	WriteRow(io.Writer) (hasData bool)
	Close()
}

type InputSplitReader interface {
	RowIterator
}

type InputFormat interface {
	GetInputSplitter(Input) InputSplitter
	GetInputSplitReader(InputSplit) (InputSplitReader, error)
	EncodeInputSplit(InputSplit) ([]byte, error)
	DecodeInputSplit(serializedInputSplit []byte) (InputSplit, error)
}

type Input interface {
	GetType() string
}
