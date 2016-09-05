package util

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"gopkg.in/vmihailenco/msgpack.v2"
)

/*
Message format:
  32 bits byte length
  []byte encoded in msgpack format

Lua scripts need to decode the input and encode the output in msgpack format.
Go code also need to decode the input to "see" the data, e.g. Sort(),
and encode the output, e.g. Source().

Shell scripts via Pipe should see clear data, so the
*/

// setup asynchronously to merge multiple channels into one channel
func MergeChannel(cs []chan []byte, out chan []byte) {
	var wg sync.WaitGroup

	for _, c := range cs {
		wg.Add(1)
		go func(c chan []byte) {
			defer wg.Done()
			for n := range c {
				out <- n
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
}

func LinkChannel(wg *sync.WaitGroup, inChan, outChan chan []byte) {
	wg.Add(1)
	defer wg.Done()
	for bytes := range inChan {
		outChan <- bytes
	}
	close(outChan)
}

func ReaderToChannel(wg *sync.WaitGroup, name string, reader io.ReadCloser, ch chan []byte, errorOutput io.Writer) {
	defer wg.Done()
	defer reader.Close()
	defer close(ch)

	var length int32

	for {
		err := binary.Read(reader, binary.LittleEndian, &length)
		if err == io.EOF {
			break
		}
		if err != nil {
			// getting this: FlatMap>Failed to read from input to channel: read |0: bad file descriptor
			// fmt.Fprintf(errorOutput, "%s>Failed to read from input to channel: %v\n", name, err)
			break
		}
		if length == 0 {
			continue
		}
		data := make([]byte, length)
		_, err = io.ReadFull(reader, data)
		if err == io.EOF {
			break // this is not really correct, but stop anyway
		}
		ch <- data
	}
}

func ChannelToWriter(wg *sync.WaitGroup, name string, ch chan []byte, writer io.WriteCloser, errorOutput io.Writer) {
	defer wg.Done()
	defer writer.Close()

	for bytes := range ch {
		if err := binary.Write(writer, binary.LittleEndian, int32(len(bytes))); err != nil {
			fmt.Fprintf(errorOutput, "%s>Failed to write length of bytes from channel to writer: %v\n", name, err)
			return
		}
		if _, err := writer.Write(bytes); err != nil {
			fmt.Fprintf(errorOutput, "%s>Failed to write bytes from channel to writer: %v\n", name, err)
			return
		}
	}
}

func LineReaderToChannel(wg *sync.WaitGroup, name string, reader io.ReadCloser, ch chan []byte, errorOutput io.Writer) {
	defer wg.Done()
	defer reader.Close()
	defer close(ch)

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		byteEncoded, err := Encode(scanner.Bytes())
		if err != nil {
			fmt.Fprintf(errorOutput, "%s>Failed to encode bytes from channel to writer: %v\n", name, err)
			return
		}
		ch <- byteEncoded
	}
	if err := scanner.Err(); err != nil {
		// TODO: what's wrong here?
		// seems the program could have ended when reading the output.
		// fmt.Fprintf(errorOutput, "Failed to read from input to channel: %v\n", err)
	}
}

func ChannelToLineWriter(wg *sync.WaitGroup, name string, ch chan []byte, writer io.WriteCloser, errorOutput io.Writer) {
	defer wg.Done()
	defer writer.Close()

	for encodedBytes := range ch {
		var bytesDecoded []byte
		var err error
		if bytesDecoded, err = Decode(encodedBytes); err != nil {
			fmt.Fprintf(errorOutput, "%s>Failed to decode bytes from channel to writer: %v\n", name, err)
			return
		}
		if _, err := writer.Write(bytesDecoded); err != nil {
			fmt.Fprintf(errorOutput, "%s>Failed to write bytes from channel to writer: %v\n", name, err)
			return
		}
		if _, err := writer.Write([]byte("\n")); err != nil {
			fmt.Fprintf(errorOutput, "%s>Failed to write len end from channel to writer: %v\n", name, err)
			return
		}
	}

}

func Encode(rawBytes []byte) ([]byte, error) {
	byteEncoded, err := msgpack.Marshal(rawBytes)
	return byteEncoded, err
}
func Decode(encodedBytes []byte) ([]byte, error) {
	// to be compatible with lua encoding, need to use string
	var bytesDecoded []byte
	err := msgpack.Unmarshal(encodedBytes, &bytesDecoded)
	return bytesDecoded, err
}
