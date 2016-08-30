// lua.go defines how an Lua script should be executed on agents.
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

func (c *ShellScript) Map(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "map",
		Code: code,
	})
}

func (c *ShellScript) Reduce(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "reduce",
		Code: code,
	})
}

func (c *ShellScript) Filter(code string) {
	c.operations = append(c.operations, &Operation{
		Type: "filter",
		Code: code,
	})
}
