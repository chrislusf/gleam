package util

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/chrislusf/gleam/pb"
)

// all data passing through pipe are all (size, msgpack_encoded) tuples
// The input and output should all be this msgpack format.
// Only the stdin and stdout of Pipe() is line based text.
func Execute(ctx context.Context, executeWaitGroup *sync.WaitGroup, stat *pb.InstructionStat,
	name string, command *exec.Cmd,
	reader io.Reader, writer io.Writer, prevIsPipe, isPipe bool, closeOutput bool,
	errWriter io.Writer) error {

	defer func() {
		executeWaitGroup.Done()
		if closeOutput {
			if c, ok := writer.(io.Closer); ok {
				c.Close()
			}
		}
	}()

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
					go ChannelToLineWriter(&wg, stat, name, reader, inputWriter, errWriter)
				} else {
					// println("step", name, "input is lines->msgpack")
					go LineReaderToChannel(&wg, stat, name, reader, inputWriter, true, errWriter)
				}
			}
		}
	}

	command.Stdout = writer

	command.Stderr = errWriter

	// println(name, "starting...", strings.Join(command.Args, ","))

	if startError := command.Start(); startError != nil {
		return fmt.Errorf("Start error %v: %v\n", startError, command)
	}

	// fmt.Printf("%s Command is waiting..\n", name)
	errChan := make(chan error)
	go func() {
		wg.Wait()
		waitError := command.Wait()
		if waitError != nil {
			waitError = fmt.Errorf("%s Wait error %+v.\n", name, waitError)
		}
		errChan <- waitError
	}()

	// defer fmt.Printf("%s Command is finished.\n", name)

	select {
	case <-ctx.Done():
		println("cancel process", command.Process.Pid, name, "...")
		command.Process.Kill()
		command.Process.Release()
		return ctx.Err()
	case err := <-errChan:
		return err
	}

}
