package util

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sync"

	"gopkg.in/vmihailenco/msgpack.v2"
)

/*
On the wire Message format, pipe:
  32 bits byte length
  []byte encoded in msgpack format

Channel Message format:
  []byte
    consequtive sections of []byte, each section is an object encoded in msgpack format

	This is not actually an array object,
	but just a consequtive list of encoded bytes for each object,
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
	BUFFER_SIZE = 1024 * 32
)

// setup asynchronously to merge multiple channels into one channel
func CopyMultipleReaders(readers []io.Reader, writer io.Writer) {
	writerChan := make(chan []byte, 16*len(readers))
	var wg sync.WaitGroup
	for i, reader := range readers {
		wg.Add(1)
		go func(i int, reader io.Reader) {
			defer wg.Done()
			ProcessMessage(reader, func(data []byte) error {
				println(i, "-> ", string(data))
				writerChan <- data
				return nil
			})
		}(i, reader)
	}
	go func() {
		wg.Wait()
		close(writerChan)
	}()
	println("ready to read data...")
	for data := range writerChan {
		println("==> ", string(data))
		WriteMessage(writer, data)
	}
}

func LinkChannel(wg *sync.WaitGroup, inChan, outChan chan []byte) {
	wg.Add(1)
	defer wg.Done()
	for bytes := range inChan {
		outChan <- bytes
	}
	close(outChan)
}

func ReaderToChannel(wg *sync.WaitGroup, name string, reader io.ReadCloser, writer io.WriteCloser, closeOutput bool, errorOutput io.Writer) {
	defer wg.Done()
	defer reader.Close()
	if closeOutput {
		defer writer.Close()
	}
	r := bufio.NewReaderSize(reader, BUFFER_SIZE)
	w := bufio.NewWriterSize(writer, BUFFER_SIZE)
	defer w.Flush()

	_, err := io.Copy(w, r)
	if err != nil {
		// getting this: FlatMap>Failed to read from input to channel: read |0: bad file descriptor
		fmt.Fprintf(errorOutput, "%s>Failed to read bytes length from input to channel: %v\n", name, err)
	}
}

func ChannelToWriter(wg *sync.WaitGroup, name string, reader io.Reader, writer io.WriteCloser, errorOutput io.Writer) {
	defer wg.Done()
	defer writer.Close()

	r := bufio.NewReaderSize(reader, BUFFER_SIZE)
	w := bufio.NewWriterSize(writer, BUFFER_SIZE)
	defer w.Flush()

	_, err := io.Copy(w, r)
	if err != nil {
		fmt.Fprintf(errorOutput, "%s> Failed to move data: %v", name, err)
	}
}

func LineReaderToChannel(wg *sync.WaitGroup, name string, reader io.ReadCloser, ch io.WriteCloser, closeOutput bool, errorOutput io.Writer) {
	defer wg.Done()
	defer reader.Close()
	if closeOutput {
		defer ch.Close()
	}

	r := bufio.NewReaderSize(reader, BUFFER_SIZE)
	w := bufio.NewWriterSize(ch, BUFFER_SIZE)
	defer w.Flush()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		// fmt.Printf("%s>line input: %s\n", name, scanner.Text())
		parts := bytes.Split(scanner.Bytes(), []byte{'\t'})
		var buf bytes.Buffer
		encoder := msgpack.NewEncoder(&buf)
		for _, p := range parts {
			if err := encoder.Encode(p); err != nil {
				if err != nil {
					fmt.Fprintf(errorOutput, "%s>Failed to encode bytes from channel to writer: %v\n", name, err)
					return
				}
			}
		}
		// fmt.Printf("%s>encoded input: %s\n", name, string(buf.Bytes()))
		WriteMessage(w, buf.Bytes())
	}
	if err := scanner.Err(); err != nil {
		// TODO: what's wrong here?
		// seems the program could have ended when reading the output.
		fmt.Fprintf(errorOutput, "Failed to read from input to channel: %v\n", err)
	}
}

func ChannelToLineWriter(wg *sync.WaitGroup, name string, reader io.Reader, writer io.WriteCloser, errorOutput io.Writer) {
	defer wg.Done()
	defer writer.Close()
	w := bufio.NewWriterSize(writer, BUFFER_SIZE)
	defer w.Flush()

	r := bufio.NewReaderSize(reader, BUFFER_SIZE)

	if err := fprintRowsFromChannel(r, w, "\t", "\n"); err != nil {
		fmt.Fprintf(errorOutput, "%s>Failed to decode bytes from channel to writer: %v\n", name, err)
		return
	}

}
