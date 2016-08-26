// lua.go defines how an Lua script should be executed on agents.
package commander

import "fmt"

type LuaCommander struct {
	initCode string
	envs     []string
}

func NewLuaCommander() *LuaCommander {
	return &LuaCommander{}
}

func (c *LuaCommander) Init(code string) {
	c.initCode = code
}

func (c *LuaCommander) Name() string {
	return "lua"
}

func (c *LuaCommander) GetCommand(code string) *Command {
	return &Command{
		Path: "luajit",
		Args: []string{"-e", c.initCode + "\n" + code},
		Envs: c.envs,
	}
}

func (c *LuaCommander) Map(code string) string {
	return fmt.Sprintf(`
		f = %s
		
	`, code)
}

func (c *LuaCommander) Reduce(code string) string {
	return code
}

func (c *LuaCommander) Filter(code string) string {
	return code
}
