package util

import (
	"io"
	"io/ioutil"
	"log"
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

func Range(from, to int) func(io.Writer) {
	return func(outChan io.Writer) {
		for i := from; i < to; i++ {
			WriteRow(outChan, i)
		}
	}
}
