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
	out := flow.New().Strings([]string{"/Users/chris/Downloads/txt/en/ep-08-*.txt"}).PipeAsArgs("cat $1")
	for i := 0; i < 30; i++ {
		out = out.Pipe("cat")
	}
	out.Fprintf(ioutil.Discard, "%s\n")
}

func testUnixPipeAsArgs() {
	// PipeAsArgs has 4ms cost to startup a process
	startTime := time.Now()
	flow.New().Source(
		util.Range(0, 100),
	).PipeAsArgs("echo foo bar $1").Fprintf(ioutil.Discard, "%s\n")

	fmt.Printf("gleam pipe time diff: %s\n", time.Now().Sub(startTime))
	fmt.Println()
}

func benchmark(name string, f func()) {
	startTime := time.Now()

	f()

	fmt.Printf("%s: %s\n", name, time.Now().Sub(startTime))
	fmt.Println()
}
