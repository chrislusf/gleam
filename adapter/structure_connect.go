package adapter

import (
	"io"
)

// AdapterQuery is any object that can be serialized by gob
type AdapterQuery interface {
	GetParallelCount() int
}

// ConnectorSplit should be serialized by gob
type Split interface {
	GetConfiguration() map[string]string
}

// Adater implenets input and output to external systems
type Adapter interface {
	AdapterName() string
	LoadConfiguration(config map[string]string)
	GetSplits(connectionId string, query AdapterQuery) ([]Split, error)
	ReadSplit(Split, io.Writer) error
}
