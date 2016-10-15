package csv

import (
	"fmt"
	"strings"

	"github.com/chrislusf/gleam/flow"
)

func Read(f *flow.FlowContext, fileNames []string, parallel int, separator string,
	hasHeader bool, fieldNames ...string) *flow.Dataset {
	if separator == "\t" {
		separator = "\\t"
	}

	readFile := fmt.Sprintf(`
        for line in csvFile:lines('*l') do
          local l = {}
          for t in split(line, '%s') do
            table.insert(l,t)
          end
          writeRow(unpack(l))
        end
    `, separator)

	if hasHeader {
		if len(fieldNames) == 0 {
			readFile = fmt.Sprintf(`
                local header = csvFile:read()

                for line in csvFile:lines() do
                  local l = {}
                  for t in split(line, '%s') do
                    table.insert(l,t)
                  end
                  writeRow(unpack(l))
                end
            `, separator)
		} else {
			readFile = fmt.Sprintf(`
                local header = csvFile:read()
                local header2position = {}
                local x = 1
                for h in split(header, '%s') do
                  header2position[h] = x
                  x = x + 1
                end
                
                local fields = {%s}
                local indexes = {}
                for _, f in pairs(fields) do
                  table.insert(indexes, header2position[f])
                end
    
                for line in csvFile:lines() do
                  local l = {}
                  for t in split(line, '%s') do
                    table.insert(l,t)
                  end
                  local row = {}
                  for _, x in ipairs(indexes) do
                    table.insert(row, l[x])
                  end
                  writeRow(unpack(row))
                end
            `, separator, toFieldNameList(fieldNames), separator)
		}
	}

	actualCode := fmt.Sprintf(`
        function(fileName)
            local csvFile = io.open(fileName, 'r')
            %s
            csvFile:close()  
        end
    `, readFile)

	return f.Strings(fileNames).Partition(parallel).ForEach(actualCode)
}

func toFieldNameList(fieldNames []string) string {
	var ret []string
	for _, f := range fieldNames {
		ret = append(ret, "\""+f+"\"")
	}
	return strings.Join(ret, ",")
}
