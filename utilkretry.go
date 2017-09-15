package util

import (
	"log"
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

	log.Printf("Retrying after failure: %v", err)

	for i, t := range waitTimes {
		time.Sleep(t)
		err = fn()
		if err == nil {
			return nil
		}
		log.Printf("Failed %d time due to %v", i+1, err)
	}

	log.Printf("Failed due to %v, gave up!", err)

	return err
}
