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

func PrintAsJSON(msgpackBytes []byte, writer io.Writer, isPrettyPrint bool) (err error) {
	var object interface{}

	decoder := NewMsgpackDecoderBytes(msgpackBytes)
	encoder := NewJSONEncoder(writer, isPrettyPrint)

	for {
		if err = decoder.Decode(&object); err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		if err = encoder.Encode(object); err != nil {
			return err
		}
	}

	return nil
}
