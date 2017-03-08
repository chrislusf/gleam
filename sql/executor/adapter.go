package executor

import (
	"fmt"
	"time"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/sql/context"
	"github.com/chrislusf/gleam/sql/infoschema"
	"github.com/chrislusf/gleam/sql/plan"
	"github.com/juju/errors"
)

// statement implements the ast.Statement interface, it builds a plan.Plan to an ast.Statement.
type Statement struct {
	// The InfoSchema cannot change during execution, so we hold a reference to it.
	InfoSchema infoschema.InfoSchema
	ctx        context.Context
	Text       string
	Plan       plan.Plan
	startTime  time.Time
}

func (a *Statement) OriginText() string {
	return a.Text
}

// Exec implements the ast.Statement Exec interface.
// This function builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned ast.RecordSet Next method.
func (a *Statement) Exec(ctx context.Context) (*flow.Dataset, error) {
	a.startTime = time.Now()

	b := newExecutorBuilder(ctx, a.InfoSchema)

	exe := b.build(a.Plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	if exe == nil {
		return nil, fmt.Errorf("Failed to build execution plan %v", plan.ToString(a.Plan))
	}

	return exe.Exec(), nil
}
