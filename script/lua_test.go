package script

import (
	"testing"
)

func TestLuaCommander(t *testing.T) {
	var commander Script

	commander = NewLuaScript()
	println(commander.Name())
}
