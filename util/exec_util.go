package util

import (
	"fmt"
	"io"
	"os/exec"
	"sync"
)

func Execute(execWaitGroup *sync.WaitGroup, name string, cmd *exec.Cmd,
	inChan, outChan chan []byte, isPipe bool, errWriter io.Writer) {

	execWaitGroup.Add(1)
	defer execWaitGroup.Done()

	var wg sync.WaitGroup

	inputWriter, stdinErr := cmd.StdinPipe()
	if stdinErr != nil {
		fmt.Fprintf(errWriter, "Failed to open StdinPipe: %v", stdinErr)
	} else {
		wg.Add(1)
		if isPipe {
			go ChannelToLineWriter(&wg, name, inChan, inputWriter, errWriter)
		} else {
			go ChannelToWriter(&wg, name, inChan, inputWriter, errWriter)
		}
	}

	outputReader, stdoutErr := cmd.StdoutPipe()
	if stdoutErr != nil {
		fmt.Fprintf(errWriter, "Failed to open StdoutPipe: %v", stdoutErr)
	} else {
		wg.Add(1)
		if isPipe {
			go LineReaderToChannel(&wg, name, outputReader, outChan, errWriter)
		} else {
			go ReaderToChannel(&wg, name, outputReader, outChan, errWriter)
		}
	}

	cmd.Stderr = errWriter

	if startError := cmd.Start(); startError != nil {
		fmt.Fprintf(errWriter, "Start error %v: %v\n", startError, cmd)
		return
	}
	if waitError := cmd.Wait(); waitError != nil {
		fmt.Fprintf(errWriter, "Wait error %v: %v\n", waitError, cmd)
	}

}
