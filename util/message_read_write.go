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
		count--
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
	for err == nil {
		count--
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
		return
	}
	if err != nil {
		fmt.Errorf("Failed to read message length: %v", err)
		return
	}
	if length == -1 {
		return nil, io.EOF
	}
	if length == 0 {
		return
	}
	m = make([]byte, length)
	_, err = io.ReadFull(reader, m)
	if err == io.EOF {
		return
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message content: %v", err)
	}
	return m, nil
}

func WriteEOFMessage(writer io.Writer) (err error) {
	if err = binary.Write(writer, binary.LittleEndian, int32(-1)); err != nil {
		return fmt.Errorf("Failed to write message length: %v", err)
	}
	return
}

func WriteMessage(writer io.Writer, m []byte) (err error) {
	if err = binary.Write(writer, binary.LittleEndian, int32(len(m))); err != nil {
		return fmt.Errorf("Failed to write message length: %v", err)
	}
	if _, err = writer.Write(m); err != nil {
		return fmt.Errorf("Failed to write message content: %v", err)
	}
	return
}
