package source

import (
	"fmt"
)

type InputFormatRegistry struct {
	nameToInputFormat map[string]InputFormat
}

var (
	Registry = &InputFormatRegistry{
		nameToInputFormat: make(map[string]InputFormat),
	}
)

func (r *InputFormatRegistry) Register(name string, format InputFormat) {
	r.nameToInputFormat[name] = format
}

func (r *InputFormatRegistry) GetInputFormat(name string) (InputFormat, error) {
	format, ok := r.nameToInputFormat[name]
	if !ok {
		return nil, fmt.Errorf("Failed to find input split reader for %s", name)
	}
	return format, nil
}

func (r *InputFormatRegistry) GetInputSplit(name string, serializedInputSplit []byte) (InputSplit, error) {
	format, ok := r.nameToInputFormat[name]
	if !ok {
		return nil, fmt.Errorf("InputFormat %s is not registered.", name)
	}
	return format.DecodeInputSplit(serializedInputSplit)
}
