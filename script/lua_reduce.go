package script

import (
	"fmt"
	"strings"
)

func (c *LuaScript) Reduce(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "Reduce",
		Code: fmt.Sprintf(`
local _reduce = %s
local row = readRow()
if row then
  local lastValue = row[1]
  while true do
    local row = readRow()
    if not row then break end
    if row[1] then
      lastValue = _reduce(lastValue, row[1]) 
    end
  end
  writeRow(lastValue)
end
`, code),
	})
}

func (c *LuaScript) ReduceBy(code string, indexes []int) {
	c.operations = append(c.operations, &Operation{
		Type: "ReduceBy",
		Code: fmt.Sprintf(`
local keyIndexes = {%s}
local keyIndexesSet = set(keyIndexes)
local keyWidth = #keyIndexes

local function _getKeysAndValues(row)
  local keys, values = listNew(), listNew()
  for i=1, row.n, 1 do
    if keyIndexesSet[i] then
      listInsert(keys, row[i])
    else
      listInsert(values, row[i])
    end
  end
  if row.n-1 == keyWidth then
    return keys, values[1]
  end
  return keys, values
end

local function _writeKeyValues(keys, values, rowWidth)
  row = listNew()
  listExtend(row, keys)
  if rowWidth-1 == keyWidth then
    listInsert(row, values)
  else
    listExtend(row, values)
  end
  writeRow(listUnpack(row))
end

local _reduce = %s

local row = readRow()
if row then
  local lastKeys, lastValues = _getKeysAndValues(row)
  while true do
    local row = readRow()
    if not row then break end
    
    local keys, values = _getKeysAndValues(row)
    if not listEquals(keys, lastKeys) then
      _writeKeyValues(lastKeys, lastValues, row.n)
      lastKeys, lastValues = _getKeysAndValues(row)
    else
      local params = listNew()
      if row.n-1 == keyWidth then
        listInsert(params, lastValues)
        listInsert(params, values)
        lastValues = _reduce(listUnpack(params))
      else
        listExtend(params, lastValues)
        listExtend(params, values)
        lastValues = listNew(_reduce(listUnpack(params)))
      end
    end
  end
  _writeKeyValues(lastKeys, lastValues, row.n)
end
`, genKeyIndexes(indexes), code),
	})
}

func (c *LuaScript) GroupBy(indexes []int) {
	c.operations = append(c.operations, &Operation{
		Type: "GroupBy",
		Code: fmt.Sprintf(`
local keyIndexes = {%s}
local keyIndexesSet = set(keyIndexes)
local keyWidth = #keyIndexes

local function _getKeysAndValues(row)
  local keys, values = listNew(), listNew()
  for i=1, row.n, 1 do
    if keyIndexesSet[i] then
      listInsert(keys, row[i])
    else
      listInsert(values, row[i])
    end
  end
  return keys, values
end

local function _writeKeyValues(keys, valuesList, rowWidth)
  row = listNew()
  listExtend(row, keys)
  if rowWidth-1 == keyWidth then
    local unpacked = listNew()
    for i=1, valuesList.n do
      listInsert(unpacked, valuesList[i][1])
    end
    listInsert(row, unpacked)
  elseif rowWidth ~= keyWidth then
    listExtend(row, valuesList)
  end
  listInsert(row, valuesList.n)
  writeRow(listUnpack(row))
end

local row = readRow()
if row then

  local lastKeys, lastValues = _getKeysAndValues(row)
  local lastValuesList = listNew(lastValues)
  while true do
    local row = readRow()
    if not row then break end

    local keys, values = _getKeysAndValues(row)
    if not listEquals(keys, lastKeys) then
      _writeKeyValues(lastKeys, lastValuesList, row.n)
      lastKeys, lastValues = _getKeysAndValues(row)
      lastValuesList = listNew(lastValues)
    else
      listInsert(lastValuesList, values)
    end
  end
  _writeKeyValues(lastKeys, lastValuesList, row.n)
end
`, genKeyIndexes(indexes)),
	})
}

func genKeyIndexes(indexes []int) (keyIndexesString string) {
	var keyFields []string
	for _, x := range indexes {
		keyFields = append(keyFields, fmt.Sprintf("%d", x))
	}
	return strings.Join(keyFields, ",")
}
