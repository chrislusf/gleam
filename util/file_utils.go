package util

import (
	"io/ioutil"
	"log"
)

func ListFiles(dir string, pattern string) (fileNames []string) {

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fileNames = append(fileNames, "/etc/"+file.Name())
	}
	return
}
