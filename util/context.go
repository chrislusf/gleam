package util

import (
	"context"
)

func ExecuteWithCleanup(parentContext context.Context, onExecute func() error, onCleanup func()) error {
	ctx, cancel := context.WithCancel(parentContext)

	errChan := make(chan error)

	go func() {
		errChan <- onExecute()
	}()

	select {
	case err := <-errChan:
		cancel()
		return err
	case <-ctx.Done():
		onCleanup()
		return ctx.Err()
	}

}
