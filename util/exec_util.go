package util

import (
	"fmt"
	"io"
	"os/exec"
	"sync"
)

func Execute(execWaitGroup *sync.WaitGroup, cmd *exec.Cmd,
	inChan, outChan chan []byte, errWriter io.Writer) {

	execWaitGroup.Add(1)
	defer execWaitGroup.Done()

	var wg sync.WaitGroup

	inputWriter, stdinErr := cmd.StdinPipe()
	if stdinErr != nil {
		fmt.Fprintf(errWriter, "Failed to open StdinPipe: %v", stdinErr)
	} else {
		wg.Add(1)
		go ChannelToWriter(&wg, inChan, inputWriter, errWriter)
	}

	outputReader, stdoutErr := cmd.StdoutPipe()
	if stdoutErr != nil {
		fmt.Fprintf(errWriter, "Failed to open StdoutPipe: %v", stdoutErr)
	} else {
		wg.Add(1)
		go ReaderToChannel(&wg, outputReader, outChan, errWriter)
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
