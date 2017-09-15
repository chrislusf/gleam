package main

import (
	"flag"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/file"
)

var (
	size          = flag.Int("size", 0, "0 for small, 1 for 1GB, 2 for 10GB")
	isDistributed = flag.Bool("distributed", false, "distributed mode or not")
	isInMemory    = flag.Bool("inMemory", true, "distributed mode but only through memory")
	isProfiling   = flag.Bool("isProfiling", false, "profiling the flow")

	splitter = gio.RegisterMapper(splitLine)
)

func main() {
	flag.Parse()
	gio.Init()

	bigFile := *size

	fileName := "/Users/chris/Desktop/record_10K_input.txt"
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

	gleamSortDistributed(fileName, size, partition, *isDistributed, *isInMemory)

}

func linuxSortDistributed(fileName string, partition int) {

	flow.New("linuxSort").Read(file.Txt(fileName, partition)).
		Map("split", splitter).
		Pipe("linuxSort", `sort -k 1`).
		MergeSortedTo("merge", 1).
		Printlnf("%s  %s").
		Run(distributed.Option())
}

func linuxSortStandalone(fileName string, partition int) {

	flow.New("linuxSort").Read(file.Txt(fileName, partition)).
		Map("split", splitter).
		Pipe("linuxSort", `sort -k 1`).
		MergeSortedTo("merge", 1).
		Printlnf("%s  %s").
		Run()

}

func gleamSortDistributed(fileName string, size int64, partition int, isDistributed, isInMemory bool) {

	f := flow.New("gleamSort").Read(file.Txt(fileName, partition)).
		Hint(flow.TotalSize(size)).
		Map("split", splitter)

	if isInMemory {
		f = f.PartitionByKey("partition", partition).SortByKey("sort")
	} else {
		f = f.OnDisk(func(d *flow.Dataset) *flow.Dataset {
			return d.PartitionByKey("partition", partition).SortByKey("sort")
		})
	}

	f = f.Printlnf("%s  %s")

	if isDistributed {
		f.Run(distributed.Option().SetProfiling(*isProfiling))
	} else {
		f.Run()
	}
}

func splitLine(row []interface{}) error {
	line := row[0].(string)
	gio.Emit(line[0:10], line[12:])
	return nil
}
