package csv

import (
	"fmt"
	"strings"

	"github.com/chrislusf/gleam/flow"
)

func Read(f *flow.FlowContext, fileNames []string, parallel int,
	hasHeader bool, fieldNames ...string) *flow.Dataset {

	declareCode := `
-- http://lua-users.org/wiki/CsvUtils
-- Used to escape "'s by toCSV
function escapeCSV (s)
  if string.find(s, '[,"]') then
    s = '"' .. string.gsub(s, '"', '""') .. '"'
  end
  return s
end

-- Convert from CSV string to table (converts a single line of a CSV file)
function fromCSV(s)
  s = s .. ','        -- ending comma
  local t = {}        -- table to collect fields
  local fieldstart = 1
  repeat
    -- next field is quoted? (start with '"'?)
    if string.find(s, '^"', fieldstart) then
      local a, c
      local i  = fieldstart
      repeat
        -- find closing quote
        a, i, c = string.find(s, '"("?)', i+1)
      until c ~= '"'    -- quote not followed by quote?
      if not i then error('unmatched "') end
      local f = string.sub(s, fieldstart+1, i-1)
      table.insert(t, (string.gsub(f, '""', '"')))
      fieldstart = string.find(s, ',', i) + 1
    elseif string.find(s, "^'", fieldstart) then
      local a, c
      local i  = fieldstart
      repeat
        -- find closing quote
        a, i, c = string.find(s, "'('?)", i+1)
      until c ~= "'"    -- quote not followed by quote?
      if not i then error("unmatched '") end
      local f = string.sub(s, fieldstart+1, i-1)
      table.insert(t, (string.gsub(f, "''", "'")))
      fieldstart = string.find(s, ',', i) + 1
    else                -- unquoted; find next comma
      local nexti = string.find(s, ',', fieldstart)
      table.insert(t, string.sub(s, fieldstart, nexti-1))
      fieldstart = nexti + 1
    end
  until fieldstart > string.len(s)
  return t
end

-- Convert from table to CSV string
function toCSV (tt)
  local s = ""
  for _,p in ipairs(tt) do  
    s = s .. "," .. escapeCSV(p)
  end
  return string.sub(s, 2)      -- remove first comma
end
`

	readFile := `
        for line in csvFile:lines('*l') do
          local l = fromCSV(line)
          writeRow(unpack(l))
        end
    `

	if hasHeader {
		if len(fieldNames) == 0 {
			readFile = `
                local header = csvFile:read()

                for line in csvFile:lines() do
                  local l = fromCSV(line)
                  writeRow(unpack(l))
                end
            `
		} else {
			readFile = fmt.Sprintf(`
                local header = fromCSV(csvFile:read())
                local header2position = {}
                for x, h in ipairs(header) do
                  header2position[h] = x
                end
                
                local fields = {%s}
                local indexes = {}
                for _, f in pairs(fields) do
                  table.insert(indexes, header2position[f])
                end
    
                for line in csvFile:lines() do
                  local l = fromCSV(line)
                  local row = {}
                  for _, x in ipairs(indexes) do
                    table.insert(row, l[x])
                  end
                  writeRow(unpack(row))
                end
            `, toFieldNameList(fieldNames))
		}
	}

	actualCode := fmt.Sprintf(`
        function(fileName)
            local csvFile = io.open(fileName, 'r')
            %s
            csvFile:close()  
        end
    `, readFile)

	return f.Strings(fileNames).Partition(parallel).Script("lua", declareCode).ForEach(actualCode)
}

func toFieldNameList(fieldNames []string) string {
	var ret []string
	for _, f := range fieldNames {
		ret = append(ret, "\""+f+"\"")
	}
	return strings.Join(ret, ",")
}
