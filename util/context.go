package util

import (
	"context"
)

func ExecuteOrCancel(parentContext context.Context, onExecute func() error, onCancel func()) error {
	ctx, cancel := context.WithCancel(parentContext)

	errChan := make(chan error)

	go func() {
		onExecute()
		errChan <- nil
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		cancel()
		onCancel()
		return ctx.Err()
	}

}
