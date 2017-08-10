package tests

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/util"
)

func TestPipePerformance(t *testing.T) {
	benchmark("unix pipe", testUnixPipeThroughput)
	benchmark("unix pipe as args", testUnixPipeAsArgs)
}

func testUnixPipeThroughput() {
	out := flow.New("unixPipe").Strings([]string{"/Users/chris/Downloads/txt/en/ep-08-*.txt"}).
		PipeAsArgs("catEach", "cat $1")
	for i := 0; i < 30; i++ {
		out = out.Pipe(fmt.Sprintf("cat%d", i), "cat")
	}
	out.Fprintf(ioutil.Discard, "%s\n")
}

func testUnixPipeAsArgs() {
	// PipeAsArgs has 4ms cost to startup a process
	startTime := time.Now()
	flow.New("unixPipeAsArgs").Source("[0,100)", util.Range(0, 100)).
		PipeAsArgs("echo", "echo foo bar $1").
		Fprintf(ioutil.Discard, "%s\n")

	fmt.Printf("gleam pipe time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func benchmark(name string, f func()) {
	startTime := time.Now()

	f()

	fmt.Printf("%s: %s\n", name, time.Now().Sub(startTime))
	fmt.Println()
}
