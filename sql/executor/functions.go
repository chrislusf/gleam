package executor

var (
	Functions = `

function _startWith(s,prefix)
   return string.sub(s,1,string.len(prefix))==prefix
end
local bit = require("bit")

function _and(x, y) return x and y end
function cast(x, typ)
  if typ == 'decimal'
  or typ == 'double'
  or typ == 'float'
  or typ == 'mediumint'
  or typ == 'int'
  or typ == 'bigint'
  or typ == 'smallint'
  or typ == 'tinyint'
  then
    return tonumber(x) or 0
  end
  if _startWith(typ, 'decimal') then
    return tonumber(x) or 0
  end
  if typ == 'text'
  or typ == 'longtext'
  or typ == 'mediumtext'
  or typ == 'char'
  or typ == 'tinytext'
  or typ == 'varchar'
  or typ == 'var_string'
  then
    return tostring(x)
  end
  return x
end
function leftshift(x, n) return bit.lshift(x, n) end
function rightshift(x, n) return bit.rshift(x, n) end
function _or(x, y) return x or y end
function ge(x, y) return x >= y end
function le(x, y) return x <= y end
function eq(x, y) return x == y end
function ne(x, y) return x ~= y end
function lt(x, y) return x < y end
function gt(x, y) return x > y end
function plus(x, y)  return x + y end
function minus(x, y) return x - y end
function bitand(x, y) return bit.band(x, y) end
function bitor(x, y)  return bit.bor(x, y)  end
function mod(x, y) return math.mod(x, y)  end
function bitxor(x, y) return bit.bitxor(x, y)  end
function div(x, y) return x / y end
function mul(x, y) return x * y end
function _not(x) return not x end
function bitneg(x)  return bit.bnot(x)  end
function intdiv(x, y)  return math.floor(x/y)  end
function xor(x, y) if x == y then return false else return true end end
function nulleq(x,y)
  if x == nil and y == nil then return true end
  return x == y
end
function unaryplus(x)  return 0+x end
function unaryminus(x) return 0-x end
function _in(...)
  local width = select('#', ...)
  local x = select(1, ...)
  for i=2, width do
    local v = select(i, ...)
    if v == x then return true end
  end
  return false
end
function isnull(x) return x == nil end

function concat(x,y) return x..y end
	`
)
