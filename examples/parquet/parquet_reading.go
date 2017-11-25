package main

import (
	. "github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/file"
)

func main() {
	gio.Init()
	f := New("Read parquet file")
	a := f.Read(file.Parquet("a.parquet", 3)).Select("select", Field(1, 2, 3, 4, 5))
	a.Printlnf("%v, %v, %v, %v, %v").Run()

}
