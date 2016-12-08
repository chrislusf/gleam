package util

import (
	"bytes"
	"fmt"
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// WriteRow encode and write a row of data
func WriteRow(outChan io.Writer, anyObject ...interface{}) error {
	encoded, err := EncodeRow(anyObject...)
	if err != nil {
		return fmt.Errorf("WriteRow encoding error: %v", err)
	}
	return WriteMessage(outChan, encoded)
}

// ReadRow read and decode one row of data
func ReadRow(ch io.Reader) (row []interface{}, err error) {
	encodedBytes, hasErr := ReadMessage(ch)
	if hasErr != nil {
		return nil, hasErr
	}
	if row, err = DecodeRow(encodedBytes); err != nil {
		return nil, fmt.Errorf("ReadRow failed to decode byte: %v", err)
	}
	return row, err
}

// EncodeRow encode one row of data to a blob
func EncodeRow(anyObject ...interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	for _, obj := range anyObject {
		if objString, isString := obj.(string); isString {
			if err := encoder.Encode([]byte(objString)); err != nil {
				return nil, err
			}
		} else if err := encoder.Encode(obj); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// EncodeRow decode one row of data from a blob
func DecodeRow(encodedBytes []byte) (objects []interface{}, err error) {
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))
	for {
		var v interface{}
		if err = decoder.Decode(&v); err != nil {
			err = fmt.Errorf("decode row error: %s\n", string(encodedBytes))
			break
		}
		objects = append(objects, v)
	}
	return objects, err
}

// DecodeRowKeysValues decode a row of data, with the indexes[] specified fields as key fields
// and the rest of fields as value fields
func DecodeRowKeysValues(encodedBytes []byte, indexes []int) (keys, values []interface{}, err error) {
	objects, err := DecodeRow(encodedBytes)
	used := make([]bool, len(objects))
	for _, x := range indexes {
		keys = append(keys, objects[x-1])
		used[x-1] = true
	}
	for i, obj := range objects {
		if !used[i] {
			values = append(values, obj)
		}
	}
	return keys, values, err

}

// DecodeRowKeys decode key fields by index[], starting from 1
func DecodeRowKeys(encodedBytes []byte, indexes []int) (keys []interface{}, err error) {
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))

	if len(indexes) == 0 {
		var key interface{}
		if err = decoder.Decode(&key); err != nil {
			err = fmt.Errorf("decode row key error: %s: %v\n", string(encodedBytes), err)
		}
		return []interface{}{key}, err
	}

	var objects []interface{}
	for m := max(indexes); m > 0; m-- {
		var v interface{}
		if err := decoder.Decode(&v); err != nil {
			return nil, fmt.Errorf("decode row error: %s\n", string(encodedBytes))
		}
		objects = append(objects, v)
	}
	for _, x := range indexes {
		keys = append(keys, objects[x-1])
	}
	return keys, err

}

func DecodeRowTo(encodedBytes []byte, objects ...interface{}) error {
	// to be compatible with lua encoding, need to use string
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))
	return decoder.Decode(objects...)
}

func max(indexes []int) int {
	m := indexes[0]
	for _, x := range indexes {
		if x > m {
			m = x
		}
	}
	return m
}
