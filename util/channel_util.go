package util

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/chrislusf/gleam/pb"
)

/*
On the wire Message format, pipe:
  32 bits byte length
  []byte encoded in msgpack format

Channel Message format:
  []byte
    consecutive sections of []byte, each section is an object encoded in msgpack format

	This is not actually an array object,
	but just a consecutive list of encoded bytes for each object,
	because msgpack can sequentially decode the objects

When used by Shell scripts:
  from input channel:
    decode the msgpack-encoded []byte into strings that's tab and '\n' separated
	and feed into the shell script
  to output channel:
    encode the tab and '\n' separated lines into msgpack-format []byte
	and feed into the output channel

When used by Lua scripts:
  from input channel:
    decode the msgpack-encoded []byte into array of objects
	and pass these objects as function parameters
  to output channel:
    encode returned objects as an array of objects, into msgpack encoded []byte
	and feed into the output channel

Output Message format:
  decoded objects

Lua scripts need to decode the input and encode the output in msgpack format.
Go code also need to decode the input to "see" the data, e.g. Sort(),
and encode the output, e.g. Source().

Shell scripts via Pipe should see clear data, so the
*/

const (
	BUFFER_SIZE = 1024 * 512
)

// setup asynchronously to merge multiple channels into one channel
func CopyMultipleReaders(readers []io.Reader, writer io.Writer) (inCounter int64, outCounter int64, e error) {

	writerChan := make(chan []byte, 16*len(readers))

	errChan := make(chan error, len(readers))
	defer close(errChan)

	var wg sync.WaitGroup
	for _, reader := range readers {
		wg.Add(1)
		go func(reader io.Reader) {
			defer wg.Done()
			err := ProcessMessage(reader, func(data []byte) error {
				writerChan <- data
				atomic.AddInt64(&inCounter, 1)
				return nil
			})
			errChan <- err
		}(reader)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for data := range writerChan {
			if err := WriteMessage(writer, data); err != nil {
				errChan <- fmt.Errorf("WriteMessage Error: %v", err)
				break
			}
			atomic.AddInt64(&outCounter, 1)
		}
	}()

	for range readers {
		err := <-errChan
		if err != nil && err != io.EOF {
			return inCounter, outCounter, err
		}
	}
	close(writerChan)

	wg.Wait()

	return inCounter, outCounter, nil
}

func ReaderToChannel(wg *sync.WaitGroup, name string, readCloser io.ReadCloser, writer io.WriteCloser, closeOutput bool, errorOutput io.Writer) error {
	defer wg.Done()
	defer readCloser.Close()
	if closeOutput {
		defer writer.Close()
	}

	var reader io.Reader = readCloser

	if _, isBufioReader := reader.(*bufio.Reader); !isBufioReader {
		reader = bufio.NewReaderSize(reader, BUFFER_SIZE)
	}
	var counter int64
	err := copyBuffer(writer, reader, &counter)
	if err != nil {
		fmt.Fprintf(errorOutput, "%s>Read %d bytes from input to channel: %v\n", name, counter, err)
		return err
	}
	// println("reader", name, "copied", n, "bytes.")
	return nil
}

func ChannelToWriter(wg *sync.WaitGroup, name string, reader io.Reader, writer io.WriteCloser, errorOutput io.Writer) error {
	defer wg.Done()
	defer writer.Close()

	if _, isBufioReader := reader.(*bufio.Reader); !isBufioReader {
		reader = bufio.NewReaderSize(reader, BUFFER_SIZE)
	}

	var counter int64
	err := copyBuffer(writer, reader, &counter)
	if err != nil {
		fmt.Fprintf(errorOutput, "%s>Moved %d bytes: %v\n", name, counter, err)
	}
	// println("writer", name, "moved", n, "bytes.")
	return err
}

func LineReaderToChannel(wg *sync.WaitGroup, stat *pb.InstructionStat, name string, reader io.Reader, ch io.WriteCloser, closeOutput bool, errorOutput io.Writer) {
	defer wg.Done()
	if closeOutput {
		defer ch.Close()
	}

	r := bufio.NewReaderSize(reader, BUFFER_SIZE)
	w := bufio.NewWriterSize(ch, BUFFER_SIZE)
	defer w.Flush()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		stat.InputCounter++
		// fmt.Printf("%s>line input: %s\n", name, scanner.Text())
		parts := bytes.Split(scanner.Bytes(), []byte{'\t'})
		var slice []interface{}
		for _, m := range parts {
			slice = append(slice, m)
		}
		NewRow(Now(), slice...).WriteTo(w)
		stat.OutputCounter++
	}
	if err := scanner.Err(); err != nil {
		// seems the program could have ended when reading the output.
		fmt.Fprintf(errorOutput, "%s>Failed to read from input to channel: %v\n", name, err)
	}
}

func ConvertLineReaderToRowReader(lineReader io.Reader, name string, errorOutput io.Writer) (rowReader io.Reader) {
	piper := NewPiper()
	go func() {
		scanner := bufio.NewScanner(lineReader)
		for scanner.Scan() {
			parts := bytes.Split(scanner.Bytes(), []byte{'\t'})
			var slice []interface{}
			for _, m := range parts {
				slice = append(slice, m)
				// fmt.Fprintf(errorOutput, "> %s\n", m)
			}
			NewRow(Now(), slice...).WriteTo(piper.Writer)
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(errorOutput, "%s>Failed to read lines to rows: %v\n", name, err)
		}
		piper.Writer.Close()
	}()
	return piper.Reader
}

func ChannelToLineWriter(wg *sync.WaitGroup, stat *pb.InstructionStat, name string, reader io.Reader, writer io.WriteCloser, errorOutput io.Writer) {
	defer wg.Done()
	defer writer.Close()
	w := bufio.NewWriterSize(writer, BUFFER_SIZE)
	defer w.Flush()

	r := bufio.NewReaderSize(reader, BUFFER_SIZE)

	if err := PrintDelimited(stat, r, w, "\t", "\n"); err != nil {
		fmt.Fprintf(errorOutput, "%s>Failed to decode bytes from channel to writer: %v\n", name, err)
		return
	}

}

func copyBuffer(dst io.Writer, src io.Reader, written *int64) (err error) {

	// this is a third buffer, additional to dst buffer and src buffer
	buf := make([]byte, BUFFER_SIZE)

	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				atomic.AddInt64(written, int64(nw))
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	return err
}
