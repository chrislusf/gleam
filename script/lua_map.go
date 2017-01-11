package script

import (
	"fmt"
	"strings"
)

func (c *LuaScript) Map(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "Map",
		Code: fmt.Sprintf(`
local _map = %s
while true do
  local row, width = readRow()
  if not row then break end

  writeRow(_map(unpack(row, 1, width)))

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

  local row, width = decodeRow(encodedBytes)
  if not row then break end

  if _filter(unpack(row, 1, width)) then
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
  local row, width = readRow()
  if not row then break end

  _foreach(unpack(row, 1, width))
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
  local row, width = readRow()
  if not row then break end

  local t = _flatMap(unpack(row, 1, width))
  if t then
    for x in t do
      writeRow(x)
    end
  end
end
`, code),
	})
}

func (c *LuaScript) Select(indexes []int) {
	max := -1
	for _, x := range indexes {
		if max < x {
			max = x
		}
	}
	var params []string
	for i := 1; i <= max; i++ {
		params = append(params, fmt.Sprintf("x%d", i))
	}
	var returns []string
	for _, x := range indexes {
		returns = append(returns, fmt.Sprintf("x%d", x))
	}
	c.Map(fmt.Sprintf(`function(%s) return %s end`,
		strings.Join(params, ","),
		strings.Join(returns, ",")))
}

func (c *LuaScript) Limit(n int) {
	c.operations = append(c.operations, &Operation{
		Type: "Limit",
		Code: fmt.Sprintf(`

local count = %d

while true do
  local row, width = readRow()
  if not row then break end
  if count > 0 then
    count = count - 1
    writeRow(unpack(row, 1, width))
  end
end
`, n),
	})
}
