package main

import (
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/gio/mapper"
	"github.com/chrislusf/gleam/gio/reducer"
	"github.com/chrislusf/gleam/plugins/file"
)

func main() {

	gio.Init()

	join1()

}

func join1() {

	f := flow.New()

	a := f.Read(file.Txt("../../flow/dataset_map.go", 1)).
		Map("tokenize", mapper.Tokenize).
		Map("addOne", mapper.AppendOne).
		ReduceBy("sum", reducer.Sum)

	b := f.Read(file.Txt("../../flow/dataset_reduce.go", 1)).
		Map("tokenize", mapper.Tokenize).
		Map("addOne", mapper.AppendOne).
		ReduceBy("sum", reducer.Sum)

	a.Join("shared words", b).Printlnf("%s\t%d\t%d")

	f.Run()

}
