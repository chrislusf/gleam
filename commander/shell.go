// lua.go defines how an Lua script should be executed on agents.
package commander

import "fmt"

type ShellCommander struct {
	initCode string
	envs     []string
}

func NewShellCommander() *ShellCommander {
	return &ShellCommander{}
}

func (c *ShellCommander) Init(code string) {
}

func (c *ShellCommander) Name() string {
	return "sh"
}

func (c *ShellCommander) GetCommand(code string) *Command {
	return &Command{
		Path: "sh",
		Args: []string{"-c", code},
		Envs: c.envs,
	}
}

func (c *ShellCommander) Map(code string) string {
	return fmt.Sprintf(`
		f = %s
		
	`, code)
}

func (c *ShellCommander) Reduce(code string) string {
	return code
}

func (c *ShellCommander) Filter(code string) string {
	return code
}
