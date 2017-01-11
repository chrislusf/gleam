// lua.go defines how an Lua script should be executed on agents.
package script

type LuaScript struct {
	luaCommand string
	initCode   string
	env        []string
	operations []*Operation
}

func NewLuaScript() Script {
	return &LuaScript{
		luaCommand: "lua",
	}
}

func NewLuajitScript() Script {
	return &LuaScript{
		luaCommand: "luajit",
	}
}

func (c *LuaScript) Init(code string) {
	c.initCode = `
local mp = require "MessagePack"
mp.set_string 'binary'
local unpack = table.unpack or unpack
local NIL = {}

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

------------ read functions ---------
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
  local width = 0
  while start <= length do
    width = width + 1
    x, start = mp.unpack(encoded, start)
    decoded[width] = x
    if start > length then break end
  end
  return decoded, width
end

function readRow()
  local encoded = readEncodedBytes()
  return decodeRow(encoded)
end

------------ write functions ---------
-- write bytes
function writeBytes(encoded)
  io.write(numbertobytes(string.len(encoded), 4))
  io.write(encoded)
end

function writeRow(...)
  local width = select('#', ...)
  local encoded = ""
  for i=1, width do
    local v = select(i, ...)
    encoded = encoded .. mp.pack(v)
    -- log(i..":"..tostring(v))
  end
  if width > 0 then
    writeBytes(encoded)
  end
end
------------ helper functions ---------
function listEquals(x, y)
  for i,v in ipairs(x) do
    if v ~= y[i] then
      return false
    end
  end
  return true
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

function split(text, sep)
  return string.gmatch(text, "([^"..sep.."]+)")
end
` + code
}

func (c *LuaScript) GetCommand() *Command {
	return &Command{
		Path: c.luaCommand,
		Args: []string{"-e", c.initCode + "\n" + c.operations[0].Code + `
io.flush()
`},
		Env: c.env,
	}
}
