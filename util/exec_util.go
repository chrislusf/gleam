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
func Execute(executeWaitGroup *sync.WaitGroup, name string, command *exec.Cmd,
	reader io.Reader, writer io.Writer, prevIsPipe, isPipe bool, closeOutput bool, errWriter io.Writer) {

	defer executeWaitGroup.Done()

	var wg sync.WaitGroup

	if reader != nil {
		if prevIsPipe && isPipe {
			// println("step", name, "input is lines->lines")
			command.Stdin = reader
		} else if !prevIsPipe && !isPipe {
			// println("step", name, "input is msgpack->msgpack")
			command.Stdin = reader
		} else {
			inputWriter, stdinErr := command.StdinPipe()
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

	command.Stdout = writer

	command.Stderr = errWriter

	// fmt.Println(name, "starting...")

	if startError := command.Start(); startError != nil {
		fmt.Fprintf(errWriter, "Start error %v: %v\n", startError, command)
		return
	}

	// fmt.Printf("%s Command is waiting..\n", name)

	wg.Wait()

	if waitError := command.Wait(); waitError != nil {
		fmt.Fprintf(errWriter, "%s Wait error %+v.\n", name, waitError)
	}

	// fmt.Println(name, "stopping output writer.")
	if closeOutput {
		if c, ok := writer.(io.Closer); ok {
			c.Close()
		}
	}

	// fmt.Printf("%s Command is finished.\n", name)

}
