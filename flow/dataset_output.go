package flow

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

// Output concurrently collects outputs from previous step to the driver.
func (d *Dataset) Output(f func(io.Reader) error) *Dataset {
	step := d.Flow.AddAllToOneStep(d, nil)
	step.IsOnDriverSide = true
	step.Name = "Output"
	step.Function = func(readers []io.Reader, writers []io.Writer, stat *pb.InstructionStat) error {
		errChan := make(chan error, len(readers))
		for i, reader := range readers {
			go func(i int, reader io.Reader) {
				errChan <- f(reader)
			}(i, reader)
		}
		for range readers {
			err := <-errChan
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to process output: %v\n", err)
				return err
			}
		}
		return nil
	}
	return d
}

// Fprintf formats using the format for each row and writes to writer.
func (d *Dataset) Fprintf(writer io.Writer, format string) *Dataset {
	fn := func(r io.Reader) error {
		w := bufio.NewWriter(writer)
		defer w.Flush()
		return util.Fprintf(w, r, format)
	}
	return d.Output(fn)
}

// Fprintlnf add "\n" at the end of each format
func (d *Dataset) Fprintlnf(writer io.Writer, format string) *Dataset {
	return d.Fprintf(writer, format+"\n")
}

// Printf prints to os.Stdout in the specified format
func (d *Dataset) Printf(format string) *Dataset {
	return d.Fprintf(os.Stdout, format)
}

// Printlnf prints to os.Stdout in the specified format,
// adding an "\n" at the end of each format
func (d *Dataset) Printlnf(format string) *Dataset {
	return d.Fprintf(os.Stdout, format+"\n")
}

// SaveFirstRowTo saves the first row's values into the operands.
func (d *Dataset) SaveFirstRowTo(decodedObjects ...interface{}) *Dataset {
	fn := func(reader io.Reader) error {
		return util.TakeMessage(reader, 1, func(encodedBytes []byte) error {
			row, err := util.DecodeRow(encodedBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				return err
			}
			var counter int
			for _, v := range row.K {
				if err := setValueTo(v, decodedObjects[counter]); err != nil {
					return err
				}
				counter++
			}
			for _, v := range row.V {
				if err := setValueTo(v, decodedObjects[counter]); err != nil {
					return err
				}
				counter++
			}

			return nil
		})
	}
	return d.Output(fn)
}

func (d *Dataset) OutputRow(f func(*util.Row) error) *Dataset {
	fn := func(reader io.Reader) error {
		return util.TakeMessage(reader, -1, func(encodedBytes []byte) error {
			row, err := util.DecodeRow(encodedBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				return err
			}
			return f(row)
		})
	}
	return d.Output(fn)
}

func setValueTo(src, dst interface{}) error {
	switch v := dst.(type) {
	case *string:
		*v = gio.ToString(src)
	case *[]byte:
		*v = src.([]byte)
	case *int:
		*v = int(gio.ToInt64(src))
	case *int8:
		*v = int8(gio.ToInt64(src))
	case *int16:
		*v = int16(gio.ToInt64(src))
	case *int32:
		*v = int32(gio.ToInt64(src))
	case *int64:
		*v = gio.ToInt64(src)
	case *uint:
		*v = uint(gio.ToInt64(src))
	case *uint8:
		*v = uint8(gio.ToInt64(src))
	case *uint16:
		*v = uint16(gio.ToInt64(src))
	case *uint32:
		*v = uint32(gio.ToInt64(src))
	case *uint64:
		*v = uint64(gio.ToInt64(src))
	case *bool:
		*v = src.(bool)
	case *float32:
		*v = float32(gio.ToFloat64(src))
	case *float64:
		*v = gio.ToFloat64(src)
	}

	v := reflect.ValueOf(dst)
	if !v.IsValid() {
		return errors.New("setValueTo nil")
	}
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("setValueTo to nonsettable %T", dst)
	}
	return nil
}
