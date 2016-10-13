package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam"
)

func main() {

	fileNames := []string{}
	filepath.Walk("../..", func(path string, f os.FileInfo, err error) error {
		if strings.HasSuffix(path, ".go") {
			fullPath, _ := filepath.Abs(path)
			fileNames = append(fileNames, fullPath)
		}
		return nil
	})

	f := gleam.New()
	word2doc := f.Strings(fileNames).Partition(7).Map(`
        function(fileName)
            local f = io.open(fileName, "rb")
            local content = f:read("*all")
            f:close()
            return content, fileName
        end
    `).Map(`
        function(content, docId)
            for word in string.gmatch(content, "%w+") do
                writeRow(word, docId, 1)
            end
        end
    `)

	termFreq := word2doc.ReduceBy(`
        function(x, y)
            return x + y
        end
    `, 1, 2)

	docFreq := termFreq.Map(`
        function(word, docId, count)
            return word, 1
        end
    `).ReduceBy(`
        function(x, y)
            return x + y
        end
    `)

	docFreq.Join(termFreq).Map(fmt.Sprintf(`
        function(word, df, docId, tf)
            return word, docId, tf, df, tf*%d/df
        end
    `, len(fileNames))).Sort(5).Fprintf(os.Stdout, "%s: %s tf=%d df=%d tf-idf=%v\n")

	f.Run()

}
