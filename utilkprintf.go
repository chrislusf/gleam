package util

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/pb"
)

// TsvPrintf reads TSV lines from reader,
// and formats according to a format specifier and writes to writer.
func TsvPrintf(writer io.Writer, reader io.Reader, format string) error {
	return TakeTsv(reader, -1, func(args []string) error {
		var objects []interface{}
		for _, arg := range args {
			objects = append(objects, arg)
		}
		if len(objects) > 0 {
			_, err := fmt.Fprintf(writer, format, objects...)
			return err
		}
		return nil
	})
}

// Fprintf reads MessagePack encoded messages from reader,
// and formats according to a format specifier and writes to writer.
func Fprintf(writer io.Writer, reader io.Reader, format string) error {

	return ProcessMessage(reader, func(encodedBytes []byte) error {
		var decodedObjects []interface{}
		var row *Row
		var err error
		// fmt.Printf("chan input encoded: %s\n", string(encodedBytes))
		if row, err = DecodeRow(encodedBytes); err != nil {
			return fmt.Errorf("Failed to decode byte: %v\n", err)
		}

		decodedObjects = append(decodedObjects, row.K...)
		decodedObjects = append(decodedObjects, row.V...)
		fmt.Fprintf(writer, format, decodedObjects...)

		return nil
	})
}

// PrintDelimited Reads and formats MessagePack encoded messages
// with delimiter and lineSeparator.
func PrintDelimited(stat *pb.InstructionStat, reader io.Reader, writer io.Writer, delimiter string, lineSperator string) error {
	return ProcessMessage(reader, func(encodedBytes []byte) error {
		var row *Row
		var err error
		// fmt.Printf("chan input encoded: %s\n", string(encodedBytes))
		if row, err = DecodeRow(encodedBytes); err != nil {
			return fmt.Errorf("Failed to decode byte: %v", err)
		}
		stat.InputCounter++

		var written = 0

		// fmt.Printf("> len=%d row:%s\n", len(decodedObjects), decodedObjects[0])
		if written, err = fprintRow(writer, 0, delimiter, row.K...); err != nil {
			return fmt.Errorf("Failed to write row: %v", err)
		}
		if _, err := fprintRow(writer, written, delimiter, row.V...); err != nil {
			return fmt.Errorf("Failed to write row: %v", err)
		}

		if _, err := writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("Failed to write line separator: %v", err)
		}
		stat.OutputCounter++
		return nil
	})
}

func fprintRow(writer io.Writer, base int, delimiter string, decodedObjects ...interface{}) (written int, err error) {
	// fmt.Printf("chan input decoded: %v\n", decodedObjects)
	for i, obj := range decodedObjects {
		if i+base != 0 {
			if _, err := writer.Write([]byte(delimiter)); err != nil {
				return written, fmt.Errorf("Failed to write tab: %v", err)
			}
		}
		// only string or []byte is allowed in piping. numbers or other types need to be converted to string
		if dat, ok := obj.(string); ok {
			if _, err := writer.Write([]byte(dat)); err != nil {
				return written, fmt.Errorf("Failed to write string: %v", err)
			}
			written++
		}
		if dat, ok := obj.([]byte); ok {
			if _, err := writer.Write(dat); err != nil {
				return written, fmt.Errorf("Failed to write bytes: %v", err)
			}
			written++
		}
	}
	return written, nil
}
