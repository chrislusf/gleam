package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
)

type MessageControl int32

const (
	MessageControlEOF = MessageControl(math.MinInt32)
)

// message contains 3 kinds of data with the formats:
//   the first 4 bytes is int32 flag
//   if flag > 0
//     actual data row bytes with length = flag
//   else if flag == math.MinInt32
//     end of partition: EOF(math.MinInt32)
//   else
//     meta data bytes with length = - flag

// TakeTsv Reads and processes TSV lines.
// If count is less than 0, all lines are processed.
func TakeTsv(reader io.Reader, count int, f func([]string) error) (err error) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if count == 0 {
			io.Copy(ioutil.Discard, reader)
			return nil
		}
		parts := bytes.Split(scanner.Bytes(), []byte{'\t'})
		var args []string
		for _, p := range parts {
			args = append(args, string(p))
		}
		if err = f(args); err != nil {
			break
		}
		count--
	}
	if err != nil {
		return fmt.Errorf("Failed to process tsv: %v\n", err)
	}
	if err = scanner.Err(); err != nil {
		return fmt.Errorf("Failed to scan tsv: %v\n", err)
	}
	return nil
}

// TakeMessage Reads and processes MessagePack encoded messages.
// If count is less than 0, all lines are processed.
func TakeMessage(reader io.Reader, count int, f func([]byte) error) (err error) {
	if _, isBufioReader := reader.(*bufio.Reader); !isBufioReader {
		reader = bufio.NewReader(reader)
	}
	for err == nil {
		if count == 0 {
			io.Copy(ioutil.Discard, reader)
			return nil
		}
		message, readError := ReadMessage(reader)
		if readError == io.EOF {
			break
		}
		if readError == nil {
			err = f(message)
		} else {
			return fmt.Errorf("Failed to read message: %v\n", readError)
		}
		count--
	}

	return
}

// ProcessMessage Reads and processes MessagePack encoded messages until EOF
func ProcessMessage(reader io.Reader, f func([]byte) error) (err error) {
	return TakeMessage(reader, -1, f)
}

// ReadMessage reads out the []byte for one message
func ReadMessage(reader io.Reader) (m []byte, err error) {
	var length int32
	err = binary.Read(reader, binary.LittleEndian, &length)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message length: %v", err)
	}
	if length == int32(MessageControlEOF) {
		return nil, io.EOF
	}
	if length == 0 {
		return
	}
	m = make([]byte, length)
	var n int
	n, err = io.ReadFull(reader, m)
	if err == io.EOF {
		return nil, fmt.Errorf("Unexpected EOF when reading message size %d, but actual only %d.", length, n)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message content size %d, but read only %d: %v", length, n, err)
	}
	return m, nil
}
