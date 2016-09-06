package util

import (
	"reflect"

	"github.com/ugorji/go/codec"
)

func NewMsgpackDecoderBytes(buf []byte) Decoder {
	h := getHandle()
	return codec.NewDecoderBytes(buf, &h)
}

func getHandle() codec.MsgpackHandle {
	h := codec.MsgpackHandle{RawToString: true}
	h.MapType = reflect.TypeOf(map[string]interface{}(nil))
	return h
}
