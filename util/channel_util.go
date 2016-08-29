package util

import (
	"bufio"
	"fmt"
	"io"
	"sync"
)

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

func ReaderToChannel(wg *sync.WaitGroup, reader io.ReadCloser, ch chan []byte, errorOutput io.Writer) {
	wg.Add(1)
	defer wg.Done()
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		ch <- scanner.Bytes()
	}
	close(ch)
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(errorOutput, "Failed to read from input to channel: %v", err)
	}
}

func ChannelToWriter(wg *sync.WaitGroup, ch chan []byte, writer io.WriteCloser, errorOutput io.Writer) {
	wg.Add(1)
	defer wg.Done()
	defer writer.Close()

	for bytes := range ch {
		if _, err := writer.Write(bytes); err != nil {
			fmt.Fprintf(errorOutput, "Failed to write bytes from channel to writer: %v", err)
		}
		if _, err := writer.Write([]byte("\n")); err != nil {
			fmt.Fprintf(errorOutput, "Failed to write len end from channel to writer: %v", err)
		}
	}

}
