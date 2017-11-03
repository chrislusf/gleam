package flow

import (
	"fmt"
	"io"
	"net"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

type Sourcer interface {
	Generate(*Flow) *Dataset
}

// Read accepts a function to read data into the flow, creating a new dataset.
// This allows custom complicated pre-built logic for new data sources.
func (fc *Flow) Read(s Sourcer) (ret *Dataset) {
	return s.Generate(fc)
}

// Listen receives textual inputs via a socket.
// Multiple parameters are separated via tab.
func (fc *Flow) Listen(network, address string) (ret *Dataset) {
	fn := func(writer io.Writer, stats *pb.InstructionStat) error {
		listener, err := net.Listen(network, address)
		if err != nil {
			return fmt.Errorf("Fail to listen on %s %s: %v", network, address, err)
		}
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("Fail to accept on %s %s: %v", network, address, err)
		}
		defer conn.Close()

		return util.TakeTsv(conn, -1, func(message []string) error {
			stats.InputCounter++
			var row []interface{}
			for _, m := range message {
				row = append(row, m)
			}
			stats.OutputCounter++
			return util.NewRow(util.Now(), row...).WriteTo(writer)
		})

	}
	return fc.Source(address, fn)
}

// Source produces data feeding into the flow.
// Function f writes to this writer.
// The written bytes should be MsgPack encoded []byte.
// Use util.EncodeRow(...) to encode the data before sending to this channel
func (fc *Flow) Source(name string, f func(io.Writer, *pb.InstructionStat) error) (ret *Dataset) {
	ret = fc.NewNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = name
	step.Function = func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		errChan := make(chan error, len(writers))
		for _, writer := range writers {
			go func(writer io.Writer) {
				errChan <- f(writer, stats)
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

// Channel accepts a channel to feed into the flow.
func (fc *Flow) Channel(ch chan interface{}) (ret *Dataset) {
	ret = fc.NewNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Channel"
	step.Function = func(readers []io.Reader, writers []io.Writer, stat *pb.InstructionStat) error {
		for data := range ch {
			stat.InputCounter++
			err := util.NewRow(util.Now(), data).WriteTo(writers[0])
			if err != nil {
				return err
			}
			stat.OutputCounter++
		}
		return nil
	}
	return
}

// Bytes begins a flow with an [][]byte
func (fc *Flow) Bytes(slice [][]byte) (ret *Dataset) {
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
func (fc *Flow) Strings(lines []string) (ret *Dataset) {
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
func (fc *Flow) Ints(numbers []int) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range numbers {
			inputChannel <- data
		}
		close(inputChannel)
	}()

	return fc.Channel(inputChannel)
}

// Slices begins a flow with an [][]interface{}
func (fc *Flow) Slices(slices [][]interface{}) (ret *Dataset) {

	ret = fc.NewNextDataset(1)
	step := fc.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Slices"
	step.Function = func(readers []io.Reader, writers []io.Writer, stat *pb.InstructionStat) error {
		for _, slice := range slices {
			stat.InputCounter++
			err := util.NewRow(util.Now(), slice).WriteTo(writers[0])
			if err != nil {
				return err
			}
			stat.OutputCounter++
		}
		return nil
	}
	return

}
