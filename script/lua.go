// lua.go defines how an Lua script should be executed on agents.
package script

import (
	"fmt"
	"strings"
)

type LuaScript struct {
	initCode   string
	env        []string
	operations []*Operation
}

func NewLuaScript() Script {
	return &LuaScript{}
}

func (c *LuaScript) Init(code string) {
	c.initCode = `
local mp = require "MessagePack"
mp.set_string 'binary'

function log(message)
  io.stderr:write(message)
  io.stderr:write("\n")
end

-- Read an integer in LSB order.
function stringtonumber(str)
  if str == nil then
      return nil
  end
  local b1, b2, b3, b4= string.byte(str, 1,4)
  local n = b1 + b2*256 + b3*65536 + b4*16777216
  n = (n > 2147483647) and (n - 4294967296) or n
  return n
end

-- Write an integer in LSB order using width bytes.
function numbertobytes(num, width)
  local function _n2b(width, num, rem)
    rem = rem * 256
    if width == 0 then return rem end
    return rem, _n2b(width-1, math.modf(num/256))
  end
  return string.char(_n2b(width-1, math.modf(num/256)))
end

-- read bytes
function readEncodedBytes()
  local block = io.read(4)
  if not block then return nil end
  local length = stringtonumber(block)
  if not length then return nil end
  local encoded = io.read(length)
  return encoded
end

function decodeRow(encoded)
  if not encoded then return nil end
  local length = string.len(encoded)
  local decoded = {}
  local start = 1
  local x = nil
  while start <= length do
    x, start = mp.unpack(encoded, start)
    table.insert(decoded, x)
    if start > length then break end
  end
  return decoded
end

function readRow()
  local encoded = readEncodedBytes()
  return decodeRow(encoded)
end

-- write bytes
function writeBytes(encoded)
  io.write(numbertobytes(string.len(encoded), 4))
  io.write(encoded)
end

function writeRow(...)
  local arg={...}
  local encoded = ""
  for i,v in ipairs(arg) do
    if i == 1 then
      encoded = mp.pack(v)
    else
      encoded = encoded .. mp.pack(v)
    end
  end
  if #arg > 0 then
    writeBytes(encoded)
  end
end

function writeUnpackedRow(keys, values, unpackValues, extra)
  if unpackValues then
    local unpacked = {}
    for i, v in ipairs(values) do
      table.insert(unpacked, v[1])
    end
    writeRow(unpack(keys), unpacked, extra)
  else
    writeRow(unpack(keys), values, extra)
  end
end

function listEquals(x, y)
  for i,v in ipairs(x) do
    if v ~= y[i] then
      return false
    end
  end
  return true
end

function tableLength(T)
  local count = 0
  for _ in pairs(T) do count = count + 1 end
  return count
end

function set(list)
  local s = {}
  for _, l in ipairs(list) do s[l] = true end
  return s
end

function addToTable(x, y)
  if not y then return end
  for _, l in ipairs(y) do table.insert(x,l) end
end

` + code
}

func (c *LuaScript) Name() string {
	return "lua"
}

func (c *LuaScript) GetCommand() *Command {
	return &Command{
		Path: "luajit",
		Args: []string{"-e", c.initCode + "\n" + c.operations[0].Code + `
io.flush()
`},
		Env: c.env,
	}
}

func (c *LuaScript) Map(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "Map",
		Code: fmt.Sprintf(`
local _map = %s
while true do
  local row = readRow()
  if not row then break end

  local t = {_map(unpack(row))}
  writeRow(unpack(t))
end
`, code),
	})
}

func (c *LuaScript) Filter(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "Filter",
		Code: fmt.Sprintf(`
local _filter = %s
while true do
  local encodedBytes = readEncodedBytes()
  if not encodedBytes then break end

  local row = decodeRow(encodedBytes)
  if not row then break end

  if _filter(unpack(row)) then
    writeBytes(encodedBytes)
  end
end
`, code),
	})
}

func (c *LuaScript) ForEach(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "ForEach",
		Code: fmt.Sprintf(`
local _foreach = %s
while true do
  local row = readRow()
  if not row then break end

  _foreach(unpack(row))
end
`, code),
	})
}

func (c *LuaScript) FlatMap(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "FlatMap",
		Code: fmt.Sprintf(`
local _flatMap = %s

while true do
  local row = readRow()
  if not row then break end

  local t = _flatMap(unpack(row))
  if t then
    for x in t do
      writeRow(x)
    end
  end
end
`, code),
	})
}

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

local row = readRow()
if row then
  local rowWidth = tableLength(row)
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

local row = readRow()
if row then
  local rowWidth = tableLength(row)

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
