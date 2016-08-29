package util

import (
	"io"
	"os/exec"
	"sync"
)

func Execute(wg *sync.WaitGroup, cmd *exec.Cmd,
	inChan, outChan chan []byte, errWriter io.Writer) {

	inputWriter, _ := cmd.StdinPipe()
	go ChannelToWriter(wg, inChan, inputWriter, errWriter)
	outputReader, _ := cmd.StdoutPipe()
	go ReaderToChannel(wg, outputReader, outChan, errWriter)
	cmd.Stderr = errWriter

	wg.Add(1)
	go func() {
		cmd.Run()
		wg.Done()
	}()

}
