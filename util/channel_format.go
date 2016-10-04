package util

import (
	"fmt"
	"io"
)

func TsvPrintf(inChan io.Reader, writer io.Writer, format string) error {
	return TakeTsv(inChan, -1, func(args []string) error {
		var objects []interface{}
		for _, arg := range args {
			objects = append(objects, arg)
		}
		_, err := fmt.Fprintf(writer, format, objects...)
		return err
	})
}

func Fprintf(inChan io.Reader, writer io.Writer, format string) error {

	return ProcessMessage(inChan, func(encodedBytes []byte) error {
		var decodedObjects []interface{}
		var err error
		// fmt.Printf("chan input encoded: %s\n", string(encodedBytes))
		if decodedObjects, err = DecodeRow(encodedBytes); err != nil {
			return fmt.Errorf("Failed to decode byte: %v\n", err)
		}

		fmt.Fprintf(writer, format, decodedObjects...)
		return nil
	})
}

func fprintRowsFromChannel(ch io.Reader, writer io.Writer, delimiter string, lineSperator string) error {
	return ProcessMessage(ch, func(encodedBytes []byte) error {
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
		return nil
	})
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
