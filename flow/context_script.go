// Package flow contains data structure for computation.
// Mostly Dataset operations such as Map/Reduce/Join/Sort etc.
package flow

import (
	"log"

	"github.com/chrislusf/gleam/script"
)

func (fc *FlowContext) Define(scriptPart string) *FlowContext {
	fc.PrevScriptPart = scriptPart
	return fc
}

func (fc *FlowContext) Script(scriptType string) *FlowContext {
	if _, ok := fc.Scripts[scriptType]; !ok {
		log.Fatalf("script type %s is not registered.", scriptType)
	}
	fc.PrevScriptType = scriptType
	return fc
}

func (fc *FlowContext) CreateScript() script.Script {
	s := fc.Scripts[fc.PrevScriptType]()
	s.Init(fc.PrevScriptPart)
	return s
}
