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
  local lastTs = row[1]
  local lastValue = row[2]
  while true do
    local row = readRow()
    if not row then break end
    if row[2] then
      lastValue = _reduce(lastValue, row[2])
      if row[1] > lastTs then
        lastTs = row[1]
      end
    end
  end
  writeRowTs(lastTs, lastValue)
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
  local ts = listUnpackTs(row)
  for i=2, row.n, 1 do
    if keyIndexesSet[i-1] then
      listInsert(keys, row[i])
    else
      listInsert(values, row[i])
    end
  end
  if row.n-2 == keyWidth then
    return ts, keys, values[1]
  end
  return ts, keys, values
end

local function _writeKeyValues(ts, keys, values, rowWidth)
  row = listNew()
  listInsert(row, ts)
  listExtend(row, keys)
  if rowWidth-2 == keyWidth then
    listInsert(row, values)
  else
    listExtend(row, values)
  end
  writeRowTs(listUnpackAll(row))
end

local _reduce = %s

local row = readRow()
if row then
  local lastTs, lastKeys, lastValues = _getKeysAndValues(row)
  while true do
    local row = readRow()
    if not row then break end
    
    local ts, keys, values = _getKeysAndValues(row)
    if not listEquals(keys, lastKeys) then
      _writeKeyValues(lastTs, lastKeys, lastValues, row.n)
      lastKeys, lastValues = keys, values
    else
      local params = listNew()
      if row.n-2 == keyWidth then
        listInsert(params, lastValues)
        listInsert(params, values)
        lastValues = _reduce(listUnpackAll(params))
      else
        listExtend(params, lastValues)
        listExtend(params, values)
        lastValues = listNew(_reduce(listUnpackAll(params)))
      end
    end

    if ts > lastTs then
      lastTs = ts
    end

  end
  _writeKeyValues(lastTs, lastKeys, lastValues, row.n)
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
  local ts = listUnpackTs(row)
  for i=2, row.n, 1 do
    if keyIndexesSet[i-1] then
      listInsert(keys, row[i])
    else
      listInsert(values, row[i])
    end
  end
  return ts, keys, values
end

local function _writeKeyValues(ts, keys, valuesList, rowWidth)
  local row = listNew()
  listInsert(row, ts)
  listExtend(row, keys)
  if rowWidth-2 == keyWidth then
    local unpacked = listNew()
    for i=1, valuesList.n do
      listInsert(unpacked, valuesList[i][1])
    end
    listInsert(row, unpacked)
  elseif rowWidth-1 ~= keyWidth then
    listExtend(row, valuesList)
  end
  listInsert(row, valuesList.n)
  writeRowTs(listUnpackAll(row))
end

local row = readRow()
if row then

  local lastTs, lastKeys, lastValues = _getKeysAndValues(row)
  local lastValuesList = listNew(lastValues)
  while true do
    local row = readRow()
    if not row then break end

    local ts, keys, values = _getKeysAndValues(row)
    if not listEquals(keys, lastKeys) then
      _writeKeyValues(lastTs, lastKeys, lastValuesList, row.n)
      lastKeys, lastValues = keys, values
      lastValuesList = listNew(lastValues)
    else
      listInsert(lastValuesList, values)
    end
    if ts > lastTs then
      lastTs = ts
    end
  end
  _writeKeyValues(lastTs, lastKeys, lastValuesList, row.n)
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
