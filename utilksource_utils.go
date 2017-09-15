package util

import (
	"io"
	"io/ioutil"
	"log"

	"github.com/chrislusf/gleam/pb"
)

func ListFiles(dir string, pattern string) (fileNames []string) {

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fileNames = append(fileNames, dir+"/"+file.Name())
	}
	return
}

func Range(from, to int) func(io.Writer, *pb.InstructionStat) error {
	return func(outWriter io.Writer, stat *pb.InstructionStat) error {
		for i := from; i < to; i++ {
			if err := NewRow(Now(), i).WriteTo(outWriter); err != nil {
				return err
			}
			stat.OutputCounter++
		}
		return nil
	}
}
