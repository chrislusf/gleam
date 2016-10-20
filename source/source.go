package source

import (
	"io"
)

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
