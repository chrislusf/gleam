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
	c.initCode = code
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
		Type: "map",
		Code: fmt.Sprintf(`
			local map = %s
		    for line in io.lines() do
				map(line)
		    end
		`, code),
	})
}

func (c *LuaScript) Reduce(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "reduce",
		Code: code,
	})
}

func (c *LuaScript) Filter(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "filter",
		Code: fmt.Sprintf(`
			local filter = %s
		    for line in io.lines() do
				if filter(line) then
					print(line)
				end
		    end
		`, code),
	})
}
