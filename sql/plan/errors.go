package plan

import (
	"github.com/chrislusf/gleam/sql/mysql"
	"github.com/chrislusf/gleam/sql/terror"
)

// Optimizer error codes.
const (
	CodeOperandColumns      terror.ErrCode = 1
	CodeInvalidWildCard     terror.ErrCode = 3
	CodeUnsupported         terror.ErrCode = 4
	CodeInvalidGroupFuncUse terror.ErrCode = 5
	CodeIllegalReference    terror.ErrCode = 6
)

// Optimizer base errors.
var (
	ErrOperandColumns              = terror.ClassOptimizer.New(CodeOperandColumns, "Operand should contain %d column(s)")
	ErrInvalidWildCard             = terror.ClassOptimizer.New(CodeInvalidWildCard, "Wildcard fields without any table name appears in wrong place")
	ErrCartesianProductUnsupported = terror.ClassOptimizer.New(CodeUnsupported, "Cartesian product is unsupported")
	ErrInvalidGroupFuncUse         = terror.ClassOptimizer.New(CodeInvalidGroupFuncUse, "Invalid use of group function")
	ErrIllegalReference            = terror.ClassOptimizer.New(CodeIllegalReference, "Illegal reference")
)

func init() {
	mySQLErrCodes := map[terror.ErrCode]uint16{
		CodeOperandColumns:      mysql.ErrOperandColumns,
		CodeInvalidWildCard:     mysql.ErrParse,
		CodeInvalidGroupFuncUse: mysql.ErrInvalidGroupFuncUse,
		CodeIllegalReference:    mysql.ErrIllegalReference,
	}
	terror.ErrClassToMySQLCodes[terror.ClassOptimizer] = mySQLErrCodes
}
