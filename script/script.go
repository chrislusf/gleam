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
	Init(code string)
	GetCommand() *Command

	Map(code string)
	ForEach(code string)
	FlatMap(code string)
	Reduce(code string)
	ReduceBy(code string, indexes []int)
	Filter(code string)
	GroupBy(indexes []int)
	Select(indexes []int)
	Limit(n int, offset int)
}

func (c *Command) ToOsExecCommand() *exec.Cmd {
	command := exec.Command(
		c.Path, c.Args...,
	)
	command.Env = c.Env
	return command
}
