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
local row, width = readRow()
if row then
  local lastValue = row[1]
  while true do
    local row, width = readRow()
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

local function _getKeysAndValues(row, rowWidth)
  local keys, values = {}, {}
  for i=1, rowWidth, 1 do
    if keyIndexesSet[i] then
      table.insert(keys, row[i])
    else
      table.insert(values, row[i])
    end
  end
  if rowWidth-1 == keyWidth then
    return keys, values[1]
  end
  return keys, values
end

local function _writeKeyValues(keys, values, rowWidth)
  row = {}
  addToTable(row, keys)
  if rowWidth-1 == keyWidth then
    table.insert(row, values)
  else
    addToTable(row, values)
  end
  writeRow(unpack(row))
end

local _reduce = %s

local row, rowWidth = readRow()
if row then
  local lastKeys, lastValues = _getKeysAndValues(row, rowWidth)
  while true do
    local row = readRow()
    if not row then break end
    
    local keys, values = _getKeysAndValues(row, rowWidth)
    if not listEquals(keys, lastKeys) then
      _writeKeyValues(lastKeys, lastValues, rowWidth)
      lastKeys, lastValues = _getKeysAndValues(row, rowWidth)
    else
      local params = {}
      if rowWidth-1 == keyWidth then
        table.insert(params, lastValues)
        table.insert(params, values)
        lastValues = _reduce(unpack(params))
      else
        addToTable(params, lastValues)
        addToTable(params, values)
        lastValues = {_reduce(unpack(params))}
      end
    end
  end
  _writeKeyValues(lastKeys, lastValues, rowWidth)
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

local function _getKeysAndValues(row, rowWidth)
  local keys, values = {}, {}
  for i=1, rowWidth, 1 do
    if keyIndexesSet[i] then
      table.insert(keys, row[i])
    else
      table.insert(values, row[i])
    end
  end
  return keys, values
end

local function _writeKeyValues(keys, valuesList, count, rowWidth)
  row = {}
  addToTable(row, keys)
  if rowWidth-1 == keyWidth then
    local unpacked = {}
    for _, values in ipairs(valuesList) do
      table.insert(unpacked, values[1])
    end
    addToTable(row, {unpacked})
  elseif rowWidth ~= keyWidth then
    addToTable(row, valuesList)
  end
  table.insert(row, count)
  writeRow(unpack(row))
end

local row, rowWidth = readRow()
if row then

  local lastKeys, lastValues = _getKeysAndValues(row, rowWidth)
  local count = 1
  local lastValuesList = {lastValues}
  while true do
    local row = readRow()
    if not row then break end

    local keys, values = _getKeysAndValues(row, rowWidth)
    if not listEquals(keys, lastKeys) then
      _writeKeyValues(lastKeys, lastValuesList, count, rowWidth)
      lastKeys, lastValues = _getKeysAndValues(row, rowWidth)
      count = 1
      lastValuesList = {lastValues}
    else
      table.insert(lastValuesList, values)
      count = count + 1
    end
  end
  _writeKeyValues(lastKeys, lastValuesList, count, rowWidth)
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
