package flow

import (
	"bufio"
	"fmt"
	"io"
	"os"

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

// PipeOut writes to writer.
// If previous step is a Pipe() or PipeAsArgs(), the output is written as is.
// Otherwise, each row of output is written in tab-separated lines.
func (d *Dataset) PipeOut(writer io.Writer) *Dataset {
	fn := func(reader io.Reader) error {
		w := bufio.NewWriter(writer)
		defer w.Flush()
		if d.Step.IsPipe {
			_, err := io.Copy(w, reader)
			return err
		}
		return util.PrintDelimited(&pb.InstructionStat{}, reader, w, "\t", "\n")
	}
	return d.Output(fn)
}

// Fprintf formats using the format for each row and writes to writer.
func (d *Dataset) Fprintf(writer io.Writer, format string) *Dataset {
	fn := func(reader io.Reader) error {
		w := bufio.NewWriter(writer)
		defer w.Flush()
		if d.Step.IsPipe {
			return util.TsvPrintf(reader, w, format)
		}
		return util.Fprintf(reader, w, format)
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
		if d.Step.IsPipe {
			return util.TakeTsv(reader, 1, func(args []string) error {
				for i, o := range decodedObjects {
					if i >= len(args) {
						break
					}
					if v, ok := o.(*string); ok {
						*v = args[i]
					} else {
						return fmt.Errorf("Should save to *string.")
					}
				}
				return nil
			})
		}

		return util.TakeMessage(reader, 1, func(encodedBytes []byte) error {
			if row, err := util.DecodeRow(encodedBytes); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				return err
			} else {
				var counter int
				for _, v := range row.K {
					counter++
					decodedObjects[counter] = v
				}
				for _, v := range row.V {
					counter++
					decodedObjects[counter] = v
				}
			}
			return nil
		})
	}
	return d.Output(fn)
}

func (d *Dataset) OutputRow(f func(*util.Row) error) *Dataset {
	fn := func(reader io.Reader) error {
		if d.Step.IsPipe {
			return util.TakeTsv(reader, -1, func(args []string) error {
				var objects []interface{}
				for _, arg := range args {
					objects = append(objects, arg)
				}
				row := util.NewRow(util.Now(), objects...)
				return f(row)
			})
		}

		return util.TakeMessage(reader, -1, func(encodedBytes []byte) error {
			if row, err := util.DecodeRow(encodedBytes); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode byte: %v\n", err)
				return err
			} else {
				return f(row)
			}
			return nil
		})
	}
	return d.Output(fn)
}
