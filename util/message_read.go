package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
)

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

func TakeMessage(reader io.Reader, count int, f func([]byte) error) (err error) {
	reader = bufio.NewReader(reader)
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

func ProcessMessage(reader io.Reader, f func([]byte) error) (err error) {
	return TakeMessage(reader, -1, f)
}

func ReadMessage(reader io.Reader) (m []byte, err error) {
	var length int32
	err = binary.Read(reader, binary.LittleEndian, &length)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message length: %v", err)
	}
	if length == -1 {
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
