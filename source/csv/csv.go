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
          local l = line:split('%s')
          writeRow(unpack(l))
        end
    `, separator)

	if hasHeader {
		if len(fieldNames) == 0 {
			readFile = fmt.Sprintf(`
                local header = csvFile:read()
    
                for line in csvFile:lines('*l') do
                  local l = line:split('%s')
                  writeRow(unpack(l))
                end
            `, separator, separator)
		} else {
			readFile = fmt.Sprintf(`
                local header = csvFile:read():split('%s')
                local fields = {%s}
                local header2position = {}
                for i, h in pairs(header) do
                  header2position[h] = i
                end
                local indexes = {}
                for _, f in pairs(fields) do
                  table.insert(indexes, header2position[f])
                end
    
                for line in csvFile:lines('*l') do
                  local l = line:split('%s')
                  local row = {}
                  for _, x in ipairs(indexes) do
                    table.insert(row, l[x])
                  end
                  writeRow(unpack(row))
                end
            `, separator, toFieldNameList(fieldNames), separator)
		}
	}
	return f.Strings(fileNames).Partition(parallel).ForEach(fmt.Sprintf(`
        function(fileName)
            local csvFile = io.open(fileName, 'r')
            %s
            csvFile:close()  
        end
    `, readFile))
}

func toFieldNameList(fieldNames) string {
	var ret []string
	for _, f := range fieldNames {
		ret = append(ret, "\""+f+"\"")
	}
	return strings.Join(ret, ",")
}
