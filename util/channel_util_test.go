package util

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
)

func TestCallingShellScript(t *testing.T) {

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

	cmd := exec.Command("sh", "-c", "grep asdf")
	Execute(&wg, cmd, ch1, ch2, os.Stderr)

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

func TestCallingLuajitScript(t *testing.T) {

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

	cmd := exec.Command("luajit", "-e", `
while true do
        local line = io.read()
        if not line then break end
        -- Without line below, script never ends
        io.write("NOOP ",line,"\n")
end
	`)
	cmd = exec.Command("luajit", "-e", `
			local mapper = function (line)
				print(line .. '$')
			end
		    for line in io.lines() do
				mapper(line)
		    end
			`)
	// cmd = exec.Command("luajit", "-e", `for line in io.lines() do io.write(line, "$$\n") end`)
	Execute(&wg, cmd, ch1, ch2, os.Stderr)

	wg.Add(1)
	go func() {
		for d := range ch2 {
			fmt.Println("ch2:", string(d))
		}
		wg.Done()
	}()

	wg.Wait()
}
