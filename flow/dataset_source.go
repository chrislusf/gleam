package flow

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/util"
)

// Listen receives textual inputs via a socket.
// Multiple parameters are separated via tab.
func (fc *FlowContext) Listen(network, address string) (ret *Dataset) {
	fn := func(writer io.Writer) error {
		listener, err := net.Listen(network, address)
		if err != nil {
			return fmt.Errorf("Fail to listen on %s %s: %v", network, address, err)
		}
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("Fail to accept on %s %s: %v", network, address, err)
		}
		defer conn.Close()
		defer util.WriteEOFMessage(writer)

		return util.TakeTsv(conn, -1, func(message []string) error {
			var row []interface{}
			for _, m := range message {
				row = append(row, m)
			}
			util.WriteRow(writer, row...)
			return nil
		})

	}
	return fc.Source(fn)
}

// ReadTsv read tab-separated lines from the reader
func (fc *FlowContext) ReadTsv(reader io.Reader) (ret *Dataset) {
	fn := func(writer io.Writer) error {
		defer util.WriteEOFMessage(writer)

		return util.TakeTsv(reader, -1, func(message []string) error {
			var row []interface{}
			for _, m := range message {
				row = append(row, m)
			}
			util.WriteRow(writer, row...)
			return nil
		})

	}
	return fc.Source(fn)
}

// Source produces data feeding into the flow.
// Function f writes to this writer.
// The written bytes should be MsgPack encoded []byte.
// Use util.EncodeRow(...) to encode the data before sending to this channel
func (fc *FlowContext) Source(f func(io.Writer) error) (ret *Dataset) {
	ret = fc.newNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Source"
	step.Function = func(readers []io.Reader, writers []io.Writer, stats *instruction.Stats) error {
		errChan := make(chan error, len(writers))
		// println("running source task...")
		for _, writer := range writers {
			go func(writer io.Writer) {
				errChan <- f(writer)
			}(writer)
		}
		for range writers {
			err := <-errChan
			if err != nil {
				return err
			}
		}
		return nil
	}
	return
}

// TextFile reads the file content as lines and feed into the flow.
// The file can be a local file or hdfs://namenode:port/path/to/hdfs/file
func (fc *FlowContext) TextFile(fname string) (ret *Dataset) {
	fn := func(writer io.Writer) error {
		w := bufio.NewWriter(writer)
		defer w.Flush()
		file, err := filesystem.Open(fname)
		if err != nil {
			return fmt.Errorf("Can not open file %s: %v", fname, err)
		}
		defer file.Close()

		reader := bufio.NewReader(file)

		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			if err := util.WriteRow(w, scanner.Bytes()); err != nil {
				return err
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Scan file %s: %v", fname, err)
			return err
		}

		return nil
	}
	return fc.Source(fn)
}

// Channel accepts a channel to feed into the flow.
func (fc *FlowContext) Channel(ch chan interface{}) (ret *Dataset) {
	ret = fc.newNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Channel"
	step.Function = func(readers []io.Reader, writers []io.Writer, stats *instruction.Stats) error {
		for data := range ch {
			err := util.WriteRow(writers[0], data)
			if err != nil {
				return err
			}
			stats.Count++
		}
		return nil
	}
	return
}

// Bytes begins a flow with an [][]byte
func (fc *FlowContext) Bytes(slice [][]byte) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range slice {
			inputChannel <- data
			// println("sent []byte of size:", len(data), string(data))
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}

// Strings begins a flow with an []string
func (fc *FlowContext) Strings(lines []string) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range lines {
			inputChannel <- []byte(data)
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}

// Ints begins a flow with an []int
func (fc *FlowContext) Ints(numbers []int) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range numbers {
			inputChannel <- data
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}

// ReadFile read files according to fileType
// The file can be on local, hdfs, s3, etc.
func (fc *FlowContext) ReadFile(source adapter.AdapterFileSource) (ret *Dataset) {
	adapterType := source.AdapterName()
	// assuming the connection id is the same as the adapter type
	adapterConnectionId := adapterType
	return fc.Query(adapterConnectionId, source)
}
