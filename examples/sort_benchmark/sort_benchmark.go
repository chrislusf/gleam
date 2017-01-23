package main

import (
	"flag"
	"os"
	"runtime/pprof"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
)

var (
	size          = flag.Int("size", 0, "0 for small, 1 for 1GB, 2 for 10GB")
	isPureGo      = flag.Bool("pureGo", true, "run with pure go or LuaJIT")
	isDistributed = flag.Bool("distributed", false, "distributed mode or not")
	isInMemory    = flag.Bool("inMemory", true, "distributed mode but only through memory")
	profFile      = flag.String("pprof", "", "profiling file output name")

	mapperId = gio.RegisterMapper(splitLine)
)

func main() {
	flag.Parse()
	gio.Init()

	if *profFile != "" {
		f, _ := os.Create(*profFile)
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	bigFile := *size

	fileName := "/Users/chris/Desktop/record_10KB_input.txt"
	partition := 2
	size := int64(1024)
	if bigFile == 1 {
		fileName = "/Users/chris/Desktop/record_1GB_input.txt"
		partition = 4
		size = 1024
	}
	if bigFile == 2 {
		fileName = "/Users/chris/Desktop/record_10GB_input.txt"
		partition = 40
		size = 10240
	}

	gleamSortDistributed(fileName, size, partition, *isPureGo, *isDistributed, *isInMemory)

}

func gleamSortStandalone(fileName string, partition int) {

	flow.New().TextFile(
		fileName,
	).Map(`
       function(line)
         return string.sub(line, 1, 10), string.sub(line, 13)
       end
   `).Partition(partition).Sort().Fprintf(os.Stdout, "%s  %s\n").Run()
}

func linuxSortDistributed(fileName string, partition int) {

	flow.New().TextFile(
		fileName,
	).Map(`
       function(line)
         return string.sub(line, 1, 10), string.sub(line, 13)
       end
    `).Partition(partition).Pipe(`
        sort -k 1
    `).MergeSortedTo(1).Fprintf(os.Stdout, "%s  %s\n").Run(distributed.Option())
}

func linuxSortStandalone(fileName string, partition int) {

	flow.New().TextFile(
		fileName,
	).Map(`
       function(line)
         return string.sub(line, 1, 10), string.sub(line, 13)
       end
    `).Partition(partition).Pipe(`
        sort -k 1
    `).MergeSortedTo(1).Fprintf(os.Stdout, "%s  %s\n").Run()
}

func gleamSortDistributed(fileName string, size int64, partition int, isPureGoMapper, isDistributed, isInMemory bool) {

	f := flow.New().TextFile(
		fileName,
	).Hint(flow.TotalSize(size))

	if isPureGoMapper {
		f = f.Mapper(mapperId)
	} else {
		f = f.Map(`
           function(line)
             return string.sub(line, 1, 10), string.sub(line, 13)
           end
       `)
	}

	if isInMemory {
		f = f.Partition(partition).Sort()
	} else {
		f = f.OnDisk(func(d *flow.Dataset) *flow.Dataset {
			return d.Partition(partition).Sort()
		})
	}

	f = f.Fprintf(os.Stdout, "%s  %s\n")

	// f.Run(distributed.Planner())
	// return

	if isDistributed {
		f.Run(distributed.Option())
	} else {
		f.Run()
	}
}

func splitLine(row []interface{}) error {
	line := row[0].([]byte)
	gio.Emit(line[0:10], line[12:])
	return nil
}
