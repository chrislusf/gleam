package util

import (
	"time"
)

func Retry(fn func() error) error {
	return TimeDelayedRetry(fn, time.Second, 3*time.Second)
}

func TimeDelayedRetry(fn func() error, waitTimes ...time.Duration) error {

	err := fn()
	if err == nil {
		return nil
	}

	for _, t := range waitTimes {
		time.Sleep(t)
		err = fn()
		if err == nil {
			return nil
		}
	}

	return err
}
