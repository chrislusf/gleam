// shell.go defines how a Shell script should be executed on agents.
package script

type Operation struct {
	Type string
	Code string
}

type ShellScript struct {
	initCode   string
	env        []string
	operations []*Operation
}

func NewShellScript() *ShellScript {
	return &ShellScript{}
}

func (c *ShellScript) Init(code string) {
}

func (c *ShellScript) Name() string {
	return "sh"
}

func (c *ShellScript) GetCommand() *Command {
	code := c.operations[0].Code
	return &Command{
		Path: "sh",
		Args: []string{"-c", code},
		Env:  c.env,
	}
}

func (c *ShellScript) Pipe(code string) *ShellScript {
	c.operations = append(c.operations, &Operation{
		Type: "Pipe",
		Code: code,
	})
	return c
}
