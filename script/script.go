// commander
package script

import (
	"os/exec"
)

type Command struct {
	Path string
	Args []string
	Env  []string
}

type Script interface {
	Name() string
	Init(code string)
	Map(code string)
	Reduce(code string)
	Filter(code string)
	GetCommand() *Command
}

func (c *Command) ToOsExecCommand() *exec.Cmd {
	cmd := exec.Command(
		c.Path, c.Args...,
	)
	cmd.Env = c.Env
	return cmd
}
