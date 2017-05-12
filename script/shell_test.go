package script

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func TestShellCat(t *testing.T) {

	data := "xxx\tyyy\t123"

	testShellScript(
		"test ls",
		func(script *ShellScript) {
			script.Pipe("cat")
		},
		func(inputWriter io.Writer) {
			fmt.Fprintln(inputWriter, data)
		},
		func(outputReader io.Reader) {
			output, err := ioutil.ReadAll(outputReader)
			if err != nil {
				fmt.Fprintf(os.Stderr, "read row error: %v", err)
				return
			}
			if string(output[0:len(output)-1]) != data {
				t.Errorf("failed cat results: %+v size:%d", string(output), len(output))
			}

		},
	)
}

func testShellScript(testName string, invokeScriptFunc func(script *ShellScript),
	inputFunc func(inputWriter io.Writer),
	outputFunc func(outputReader io.Reader)) {

	shellScript := NewShellScript()
	shellScript.Init("")

	input, output := util.NewPiper(), util.NewPiper()

	invokeScriptFunc(shellScript)

	var wg sync.WaitGroup
	wg.Add(1)
	go util.Execute(context.Background(), &wg, &pb.InstructionStat{}, testName, shellScript.GetCommand().ToOsExecCommand(), input.Reader, output.Writer, false, false, true, os.Stderr)

	wg.Add(1)
	go func() {
		defer wg.Done()
		inputFunc(input.Writer)
		input.Writer.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		outputFunc(output.Reader)
	}()

	wg.Wait()
}
