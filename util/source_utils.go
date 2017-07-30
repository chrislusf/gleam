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

func Range(from, to int) func(io.Writer) error {
	return func(outWriter io.Writer) error {
		for i := from; i < to; i++ {
			if err := NewRow(Now(), i).WriteTo(outWriter); err != nil {
				return err
			}
		}
		return nil
	}
}
