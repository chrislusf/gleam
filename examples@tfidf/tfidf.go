package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/gio/mapper"
	"github.com/chrislusf/gleam/gio/reducer"
	"github.com/chrislusf/gleam/util"
)

var (
	registeredReadConent = gio.RegisterMapper(readContent)
	registeredTfIdf      = gio.RegisterMapper(tfidf)
	isDistributed        = flag.Bool("distributed", false, "run in distributed or not")
)

func main() {

	gio.Init()
	flag.Parse()

	fileNames := []string{}
	filepath.Walk("..", func(path string, f os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".go") {
			fullPath, _ := filepath.Abs(path)
			fileNames = append(fileNames, fullPath)
		}
		return nil
	})

	f := flow.New("tfidf")
	word_doc_one := f.Strings(fileNames).
		PartitionByKey("partition", 7).
		Map("read content", registeredReadConent)

	termFreq :=
		word_doc_one.ReduceBy("word_doc_tf", reducer.SumInt64, flow.Field(1, 2))

	docFreq := termFreq.
		Select("word doc freq", flow.Field(1)).
		Map("appendOne", mapper.AppendOne).
		ReduceBy("df", reducer.SumInt64, flow.Field(1))

	docFreq.Join("byWord", termFreq, flow.Field(1)).
		Map("tfidf", registeredTfIdf).
		// Sort("sort by tf/df", flow.Field(5)).
		OutputRow(func(row *util.Row) error {
			fmt.Printf("%s: %s tf=%d df=%d tf-idf=%f\n",
				row.K[0],
				row.V[0],
				row.V[1].(uint16),
				row.V[2].(uint16),
				row.V[3].(float32)/float32(len(fileNames)),
			)
			return nil
		})

	if *isDistributed {
		f.Run(distributed.Option())
	} else {
		f.Run()
	}

}

func readContent(x []interface{}) error {

	filepath := gio.ToString(x[0])

	f, err := os.Open(filepath)
	if err != nil {
		println("error reading file:", filepath)
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		gio.Emit(scanner.Text(), filepath, 1)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}
	return nil
}

func tfidf(x []interface{}) error {
	fmt.Fprintf(os.Stderr, "tfidf input: %v\n", x)
	word := gio.ToString(x[0])
	df := uint16(gio.ToInt64(x[1]))
	doc := gio.ToString(x[2])
	tf := uint16(gio.ToInt64(x[3]))

	gio.Emit(word, doc, tf, df, float32(tf)/float32(df))
	return nil
}
