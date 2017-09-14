package util

import (
	"bytes"
	"fmt"
	"io"

	"github.com/glycerine/truepack/msgp"
)

// WriteTo encode and write a row of data to the writer
func (row Row) WriteTo(writer io.Writer) (err error) {
	encoded, err := encodeRow(row)
	if err != nil {
		return fmt.Errorf("WriteTo encoding error: %v", err)
	}
	return WriteMessage(writer, encoded)
}

// ReadRow read and decode one row of data
func ReadRow(reader io.Reader) (row *Row, err error) {
	encodedBytes, hasErr := ReadMessage(reader)
	if hasErr != nil {
		if hasErr != io.EOF {
			return row, fmt.Errorf("ReadRow ReadMessage: %v", hasErr)
		}
		return row, io.EOF
	}
	if row, err = DecodeRow(encodedBytes); err != nil {
		return row, fmt.Errorf("ReadRow failed to decode byte: %v", err)
	}
	return row, err
}

// EncodeRow encode one row of data to a blob
func encodeRow(row Row) ([]byte, error) {
	return row.MarshalMsg(nil)
}

// EncodeKeys encode keys to a blob, for comparing or sorting
func EncodeKeys(anyObject ...interface{}) ([]byte, error) {
	var buf bytes.Buffer
	en := msgp.NewWriter(&buf)
	for _, obj := range anyObject {
		if err := en.WriteIntf(obj); err != nil {
			return nil, fmt.Errorf("Failed to encode key: %v", err)
		}
	}
	return buf.Bytes(), nil
}

// DecodeRow decodes one row of data from a blob
func DecodeRow(encodedBytes []byte) (*Row, error) {
	row := &Row{}
	_, err := row.UnmarshalMsg(encodedBytes)
	if err != nil {
		err = fmt.Errorf("decode row error %v: %s\n", err, string(encodedBytes))
	}
	return row, err
}

// ProcessRow Reads and processes rows until EOF
func ProcessRow(reader io.Reader, indexes []int, f func(*Row) error) (err error) {
	return ProcessMessage(reader, func(input []byte) error {
		// read the row
		row, err := DecodeRow(input)
		if err != nil {
			return fmt.Errorf("DoLocalDistinct error %v: %+v", err, input)
		}
		row.UseKeys(indexes)
		return f(row)
	})
}
