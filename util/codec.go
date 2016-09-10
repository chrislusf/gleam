package util

import (
	"io"
)

type Encoder interface {
	Encode(v interface{}) error
}

type Decoder interface {
	Decode(v interface{}) error
}

func DecodeToObject(msgpackBytes []byte) (object interface{}, err error) {

	decoder := NewMsgpackDecoderBytes(msgpackBytes)

	err = decoder.Decode(&object)

	return
}

func PrintAsJSON(object interface{}, writer io.Writer, isPrettyPrint bool) error {
	encoder := NewJSONEncoder(writer, isPrettyPrint)
	if err := encoder.Encode(object); err != nil {
		return err
	}

	return nil
}
