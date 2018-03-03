package util

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"testing"
	"time"
)

func TestCopyBuffer(t *testing.T) {

	// copying via a small buffer for testing
	bufSize := 10

	var input = bytes.NewBuffer(nil)
	var output = bytes.NewBuffer(nil)

	err := WriteMessage(input, []byte("0123456789012"))
	if err != nil {
		t.Errorf("WriteMessage 1: %v", err)
	}
	err = WriteMessage(input, []byte("1123456789012"))
	if err != nil {
		t.Errorf("WriteMessage 2: %v", err)
	}

	var reader io.Reader = input

	if _, isBufioReader := reader.(*bufio.Reader); !isBufioReader {
		reader = bufio.NewReaderSize(reader, bufSize)
	}

	// buf := make([]byte, bufSize)
	var counter int64
	err = copyBuffer(output, reader, &counter)
	if err != nil {
		log.Printf("Moved %d bytes: %v\n", counter, err)
	}
	println("copy buffer completed: ", counter)

	time.Sleep(100 * time.Millisecond)

	var lineCounter int
	err = ProcessMessage(output, func(b []byte) error {
		lineCounter++
		println("processed message", lineCounter, ":", string(b), ".")
		return nil
	})
	if err != nil {
		t.Errorf("ProcessMessage: %v", err)
	}

	if lineCounter != 2 {
		t.Errorf("line count: %d, expecting %d", lineCounter, 2)
	}

}
