package flow

import (
	"log"

	"github.com/chrislusf/gleam/script"
)

// Init defines or declares variables or functions for the script.
// This piece of code is executed first, before each function that
// invokes a script.
func (fc *FlowContext) Init(scriptPart string) *FlowContext {
	fc.PrevScriptPart = scriptPart
	return fc
}

// Script defines the code to execute to generate the next dataset.
func (fc *FlowContext) Script(scriptType string) *FlowContext {
	if _, ok := fc.Scripts[scriptType]; !ok {
		log.Fatalf("script type %s is not registered.", scriptType)
	}
	fc.PrevScriptType = scriptType
	return fc
}

func (fc *FlowContext) createScript() script.Script {
	s := fc.Scripts[fc.PrevScriptType]()
	s.Init(fc.PrevScriptPart)
	return s
}
