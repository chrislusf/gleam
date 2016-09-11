// lua.go defines how an Lua script should be executed on agents.
package script

import (
	"fmt"
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
  writeBytes(encoded)
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

local lastKey = nil
local lastValue = nil
while true do
  local row = readRow()
  if not row then break end

  if row[1] ~= lastKey then
    if lastKey then
      writeRow(lastKey, lastValue)
    end
    lastKey, lastValue = row[1], row[2]
  else
    if not lastValue then lastValue = row[2] end
    if row[2] then
      lastValue = _reduce(lastValue, row[2]) 
    end
  end
end
writeRow(lastKey, lastValue)
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

  if _filter(row[1]) then
    writeBytes(encodedBytes)
  end
end
`, code),
	})
}

func (c *LuaScript) GroupByKey() {
	c.operations = append(c.operations, &Operation{
		Type: "GroupByKey",
		Code: fmt.Sprintf(`
local lastKey = nil
local lastValues = {}
while true do
  local row = readRow()
  if not row then break end

  if row[1] ~= lastKey then
    if lastKey then
      writeRow(lastKey, lastValues)
    end
    lastKey, lastValues = row[1], {row[2]}
  else
    table.insert(lastValues, row[2])
  end
end
if #lastValues > 0 then
  writeRow(lastKey, lastValues)
end
`),
	})
}
