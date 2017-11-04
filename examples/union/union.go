package main

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/file"
)

func runEnableParallel() {
	f := flow.New("Union two Dataset isParallel=true")
	datasetOri := f.Read(file.Txt("/etc/passwd", 1)).Pipe("sort", "sort")
	datasetA := datasetOri.Pipe("field user", "awk -F ':' '{print $1}'")
	datasetB := datasetOri.Pipe("field uid", "awk -F ':' '{print $3}'")
	datasetC := datasetOri.Pipe("field home", "awk -F ':' '{print $6}'")
	// if isParallel=true
	// datasetOut reading from datasetA/datasetB/datasetC competition
	// so shards in datasetOut get rows NOT in the order of datasetA/datasetB/datasetC
	datasetOut := datasetA.Union("union", []*flow.Dataset{datasetB, datasetC}, true)
	datasetOut.Printlnf("%s").Run()
}

func runDisableParallel() {
	f := flow.New("Union two Dataset isParallel=false")
	datasetOri := f.Read(file.Txt("/etc/passwd", 1)).Pipe("sort", "sort")
	datasetA := datasetOri.Pipe("field user", "awk -F ':' '{print $1}'")
	datasetB := datasetOri.Pipe("field uid", "awk -F ':' '{print $3}'")
	datasetC := datasetOri.Pipe("field home", "awk -F ':' '{print $6}'")
	// if isParallel=false
	// datasetOut reading from datasetA/datasetB/datasetC in order,
	//     that means it must ALL data in datasetA are readed, then datasetB can be reading
	// so shards in datasetOut get rows in the order of datasetA/datasetB/datasetC
	datasetOut := datasetA.Union("union", []*flow.Dataset{datasetB, datasetC}, false)
	datasetOut.Printlnf("%s").Run()
}

func main() {
	gio.Init()

	fmt.Println("================================================================")
	fmt.Println("==== Union isParallel=true ====")
	runEnableParallel()
	fmt.Println("================================================================")
	fmt.Println("==== Union isParallel=false ====")
	runDisableParallel()
	fmt.Println("================================================================")

}
