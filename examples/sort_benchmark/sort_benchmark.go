package main

import (
	"os"
	"runtime/pprof"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
)

func main() {
	f, _ := os.Create("p.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	gleamSortDistributed()

}

func gleamSortStandalone() {

	flow.New().TextFile(
		"/Users/chris/Desktop/record_1Gb_input.txt",
	).Map(`
       function(line)
         return string.sub(line, 1, 10), string.sub(line, 13)
       end
   `).Partition(4).Sort().Fprintf(os.Stdout, "%s  %s\n").Run()
}

func linuxSortDistributed() {

	flow.New().TextFile(
		"/Users/chris/Desktop/record_1Gb_input.txt",
	).Map(`
       function(line)
         return string.sub(line, 1, 10), string.sub(line, 13)
       end
    `).Partition(4).Pipe(`
        sort -k 1
    `).MergeSortedTo(1).Fprintf(os.Stdout, "%s  %s\n").Run(distributed.Option())
}

func linuxSortStandalone() {

	flow.New().TextFile(
		"/Users/chris/Desktop/record_1Gb_input.txt",
	).Map(`
       function(line)
         return string.sub(line, 1, 10), string.sub(line, 13)
       end
    `).Partition(4).Pipe(`
        sort -k 1
    `).MergeSortedTo(1).Fprintf(os.Stdout, "%s  %s\n").Run()
}

func gleamSortDistributed() {

	flow.New().TextFile(
		"/Users/chris/Desktop/record_1Gb_input.txt",
	).Hint(flow.TotalSize(1024)).Map(`
       function(line)
         return string.sub(line, 1, 10), string.sub(line, 13)
       end
   `).Partition(4).Sort().Fprintf(os.Stdout, "%s  %s\n").Run(distributed.Option())
}
