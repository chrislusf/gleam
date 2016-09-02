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
function readBytes()
	local block = io.read(4)
	if not block then return nil end
	local length = stringtonumber(block)
	if not length then return nil end
	local line = io.read(length)
	return line
end

-- write bytes
function writeBytes(line)
	io.write(numbertobytes(string.len(line), 4))
	io.write(line)
end
` + code
}

func (c *LuaScript) Name() string {
	return "lua"
}

func (c *LuaScript) GetCommand() *Command {
	return &Command{
		Path: "luajit",
		Args: []string{"-e", c.initCode + "\n" + c.operations[0].Code},
		Env:  c.env,
	}
}

func (c *LuaScript) Map(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "Map",
		Code: fmt.Sprintf(`
			local map = %s
			while true do
				local line = readBytes()
				if not line then break end

				local t = map(line)
				writeBytes(t)
			end
		`, code),
	})
}

func (c *LuaScript) FlatMap(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "FlatMap",
		Code: fmt.Sprintf(`
			local map = %s
			while true do
				local line = readBytes()
				if not line then break end

				local t = map(line)
				for x in t do
					writeBytes(x)
				end
			end
		`, code),
	})
}

func (c *LuaScript) Reduce(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "Reduce",
		Code: code,
	})
}

func (c *LuaScript) Filter(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "Filter",
		Code: fmt.Sprintf(`
			local filter = %s
			while true do
				local line = readBytes()
				if not line then break end

				if filter(line) then
					writeBytes(line)
				end
			end
		`, code),
	})
}
