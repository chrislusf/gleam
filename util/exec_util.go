package util

import (
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// all data passing through pipe are all (size, msgpack_encoded) tuples
// The input and output should all be this msgpack format.
// Only the stdin and stdout of Pipe() is line based text.
func Execute(executeWaitGroup *sync.WaitGroup, name string, cmd *exec.Cmd,
	reader io.Reader, writer io.Writer, prevIsPipe, isPipe bool, closeOutput bool, errWriter io.Writer) {

	defer executeWaitGroup.Done()

	var wg sync.WaitGroup

	if reader != nil {
		if prevIsPipe && isPipe {
			// println("step", name, "input is lines->lines")
			cmd.Stdin = reader
		} else if !prevIsPipe && !isPipe {
			// println("step", name, "input is msgpack->msgpack")
			cmd.Stdin = reader
		} else {
			inputWriter, stdinErr := cmd.StdinPipe()
			if stdinErr != nil {
				fmt.Fprintf(errWriter, "Failed to open StdinPipe: %v", stdinErr)
			} else {
				wg.Add(1)
				if !prevIsPipe && isPipe {
					// println("step", name, "input is msgpack->lines")
					go ChannelToLineWriter(&wg, name, reader, inputWriter, errWriter)
				} else {
					// println("step", name, "input is lines->msgpack")
					go LineReaderToChannel(&wg, name, reader, inputWriter, true, errWriter)
				}
			}
		}
	}

	cmd.Stdout = writer

	cmd.Stderr = errWriter

	// fmt.Println(name, "starting...")

	if startError := cmd.Start(); startError != nil {
		fmt.Fprintf(errWriter, "Start error %v: %v\n", startError, cmd)
		return
	}

	// fmt.Printf("Command is waiting: %v\n", cmd)

	wg.Wait()

	if waitError := cmd.Wait(); waitError != nil {
		fmt.Fprintf(errWriter, "Wait error %+v. command:%+v\n", waitError, cmd)
	}

	// fmt.Printf("Command is finished.\n %+v\n", cmd)

	// fmt.Println(name, "stopping output writer.")
	if closeOutput {
		if c, ok := writer.(io.Closer); ok {
			c.Close()
		}
	}
}
