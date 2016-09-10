package util

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
)

func xTestCallingShellScript(t *testing.T) {

	data := [][]byte{
		[]byte("asdf"),
		[]byte("hlkgjh"),
		[]byte("truytyu"),
		[]byte("34weqrqw"),
		[]byte("asdfadfasaf"),
	}

	var wg sync.WaitGroup
	ch1 := make(chan []byte)
	ch2 := make(chan []byte)

	wg.Add(1)
	go func() {
		fmt.Println("starting sending to ch1 ...")
		for _, d := range data {
			ch1 <- d
		}
		close(ch1)
		wg.Done()
	}()

	wg.Add(1)
	cmd := exec.Command("sh", "-c", "grep asdf")
	Execute(&wg, "testing shell", cmd, ch1, ch2, true, os.Stderr)

	wg.Add(1)
	go func() {
		fmt.Println("start reading ch2...")
		for d := range ch2 {
			fmt.Println("ch2:", string(d))
		}
		wg.Done()
	}()

	wg.Wait()
}

func xTestCallingLuajitScript(t *testing.T) {

	data := [][]byte{
		[]byte("asdf"),
		[]byte("hlkgjh"),
		[]byte("truytyu"),
		[]byte("34weqrqw"),
		[]byte("asdfadfasaf"),
	}

	var wg sync.WaitGroup
	ch1 := make(chan []byte)
	ch2 := make(chan []byte)

	wg.Add(1)
	go func() {
		for _, d := range data {
			ch1 <- d
		}
		close(ch1)
		wg.Done()
	}()

	wg.Add(1)
	cmd := exec.Command("luajit", "-e", `
			local mapper = function (line)
				print(line .. '$')
			end
		    for line in io.lines() do
				mapper(line)
		    end
			`)
	Execute(&wg, "testing luajit", cmd, ch1, ch2, true, os.Stderr)

	wg.Add(1)
	go func() {
		for d := range ch2 {
			fmt.Println("ch2:", string(d))
		}
		wg.Done()
	}()

	wg.Wait()
}
