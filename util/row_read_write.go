package util

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// Row format
// ts, obj1, obj2, obj3, ...

// WriteRow encode and write a row of data
func WriteRow(writer io.Writer, ts int64, anyObject ...interface{}) error {
	encoded, err := EncodeRow(ts, anyObject...)
	if err != nil {
		return fmt.Errorf("WriteRow encoding error: %v", err)
	}
	return WriteMessage(writer, encoded)
}

// ReadRow read and decode one row of data
func ReadRow(reader io.Reader) (ts int64, row []interface{}, err error) {
	encodedBytes, hasErr := ReadMessage(reader)
	if hasErr != nil {
		return 0, nil, fmt.Errorf("ReadRow ReadMessage: %v", hasErr)
	}
	if ts, row, err = DecodeRow(encodedBytes); err != nil {
		return ts, row, fmt.Errorf("ReadRow failed to decode byte: %v", err)
	}

	return ts, row, err
}

// EncodeRow encode one row of data to a blob
func EncodeRow(ts int64, anyObject ...interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	encoder.EncodeInt64(ts)
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

// DecodeRow decodes one row of data from a blob
func DecodeRow(encodedBytes []byte) (ts int64, objects []interface{}, err error) {
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))

	for {
		var v interface{}
		if err = decoder.Decode(&v); err != nil {
			if err == io.EOF {
				return toInt64(objects[0]), objects[1:], nil
			}
			err = fmt.Errorf("decode row error %v: %s\n", err, string(encodedBytes))
			break
		}
		objects = append(objects, v)
	}

	return toInt64(objects[0]), objects[1:], err
}

// DecodeRowKeysValues decode a row of data, with the indexes[] specified fields as key fields
// and the rest of fields as value fields
func DecodeRowKeysValues(encodedBytes []byte, indexes []int) (ts int64, keys, values []interface{}, err error) {
	ts, objects, err := DecodeRow(encodedBytes)
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
	return ts, keys, values, err

}

// DecodeRowKeys decode key fields by index[], starting from 1
func DecodeRowKeys(encodedBytes []byte, indexes []int) (keys []interface{}, err error) {
	decoder := msgpack.NewDecoder(bytes.NewReader(encodedBytes))

	var ts int64
	decoder.Decode(&ts)

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
	var ts int64
	decoder.Decode(&ts)
	return decoder.Decode(objects...)
}

func Now() (ts int64) {
	return time.Now().UnixNano() / int64(time.Millisecond)
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

func toInt64(val interface{}) int64 {
	if v, ok := val.(int64); ok {
		return int64(v)
	}
	if v, ok := val.(uint64); ok {
		return int64(v)
	}
	if v, ok := val.(int32); ok {
		return int64(v)
	}
	if v, ok := val.(uint32); ok {
		return int64(v)
	}
	if v, ok := val.(int); ok {
		return int64(v)
	}
	if v, ok := val.(uint); ok {
		return int64(v)
	}
	if v, ok := val.(int16); ok {
		return int64(v)
	}
	if v, ok := val.(uint16); ok {
		return int64(v)
	}
	if v, ok := val.(int8); ok {
		return int64(v)
	}
	if v, ok := val.(uint8); ok {
		return int64(v)
	}
	return 0
}
