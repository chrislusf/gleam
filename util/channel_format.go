package util

import (
	"fmt"
	"io"
	"os"
)

func Fprintf(inChan chan []byte, writer io.Writer, format string) {

	for encodedBytes := range inChan {
		var decodedObjects []interface{}
		var err error
		// fmt.Printf("chan input encoded: %s\n", string(encodedBytes))
		if decodedObjects, err = DecodeRow(encodedBytes); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
			continue
		}

		fmt.Fprintf(writer, format, decodedObjects...)
	}
}

func fprintRowsFromChannel(ch chan []byte, writer io.Writer, delimiter string, lineSperator string) error {
	for encodedBytes := range ch {
		var decodedObjects []interface{}
		var err error
		// fmt.Printf("chan input encoded: %s\n", string(encodedBytes))
		if decodedObjects, err = DecodeRow(encodedBytes); err != nil {
			return fmt.Errorf("Failed to decode byte: %v", err)
		}

		// fmt.Printf("> len=%d row:%s\n", len(decodedObjects), decodedObjects[0])
		if err := fprintRow(writer, "\t", decodedObjects...); err != nil {
			return fmt.Errorf("Failed to write row: %v", err)
		}

		if _, err := writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("Failed to write line separator: %v", err)
		}
	}
	return nil
}

func fprintRow(writer io.Writer, delimiter string, decodedObjects ...interface{}) error {
	// fmt.Printf("chan input decoded: %v\n", decodedObjects)
	for i, obj := range decodedObjects {
		if i != 0 {
			if _, err := writer.Write([]byte(delimiter)); err != nil {
				return fmt.Errorf("Failed to write tab: %v", err)
			}
		}
		// only string or []byte is allowed in piping. numbers or other types need to be converted to string
		if dat, ok := obj.(string); ok {
			if _, err := writer.Write([]byte(dat)); err != nil {
				return fmt.Errorf("Failed to write string: %v", err)
			}
		}
		if dat, ok := obj.([]byte); ok {
			if _, err := writer.Write(dat); err != nil {
				return fmt.Errorf("Failed to write bytes: %v", err)
			}
		}
	}
	return nil
}
