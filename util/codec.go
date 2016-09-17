package util

import (
	"bytes"
	"fmt"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
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

func WriteRow(outChan chan []byte, anyObject ...interface{}) error {
	encoded, err := EncodeRow(anyObject...)
	if err != nil {
		return fmt.Errorf("WriteRow encoding error: %v", err)
	}
	outChan <- encoded
	return nil
}

func ReadRow(ch chan []byte) (row []interface{}, err error) {
	encodedBytes, ok := <-ch
	if !ok {
		return nil, io.EOF
	}
	if row, err = DecodeRow(encodedBytes); err != nil {
		return nil, fmt.Errorf("ReadRow failed to decode byte: %v", err)
	}
	return row, err
}

func EncodeRow(anyObject ...interface{}) ([]byte, error) {
	byteEncoded, err := msgpack.Marshal(anyObject...)
	return byteEncoded, err
}

func DecodeRow(encodedBytes []byte) (objects []interface{}, err error) {
	// to be compatible with lua encoding, need to use string
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))
	for {
		var v interface{}
		if err := decoder.Decode(&v); err != nil {
			err = fmt.Errorf("decode row error: %s\n", string(encodedBytes))
			break
		}
		objects = append(objects, v)
	}
	return objects, err
}

func DecodeRowKey(encodedBytes []byte) (key interface{}, err error) {
	// to be compatible with lua encoding, need to use string
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))
	if err = decoder.Decode(&key); err != nil {
		err = fmt.Errorf("decode row key error: %s: %v\n", string(encodedBytes), err)
	}
	return key, err
}

func DecodeRowTo(encodedBytes []byte, objects ...interface{}) error {
	// to be compatible with lua encoding, need to use string
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))
	return decoder.Decode(objects...)
}
