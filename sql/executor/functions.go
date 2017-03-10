package executor

var (
	Functions = `

local bit = require("bit")

function _and(x, y) return x and y end
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

	`
)
