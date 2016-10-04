package util

import (
	"fmt"
	"io"
	"io/ioutil"
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
	ch1 := NewPiper(true)
	ch2 := NewPiper(true)

	wg.Add(1)
	go func() {
		fmt.Println("starting sending to ch1 ...")
		for _, d := range data {
			//WriteRow(ch1.Writer, d)
			ch1.Writer.Write(d)
			ch1.Writer.Write([]byte("\n"))
		}
		//fmt.Println("stopped sending to ch1.")
		ch1.Writer.Close()
		wg.Done()
	}()

	wg.Add(1)
	cmd := exec.Command("sh", "-c", "cat")
	go Execute(&wg, "testing shell", cmd, ch1, ch2, true, false, os.Stderr)

	if false {
		var counter int
		wg.Add(1)
		go func() {
			fmt.Println("start reading ch2...")
			ProcessMessage(ch2.Reader, func(d []byte) error {
				row, _ := DecodeRow(d)
				counter++
				fmt.Println("ch2:", string(row[0].([]byte)))
				return nil
			})
			// fmt.Println("stopped reading ch2.")
			wg.Done()
		}()
	} else {
		if false {
			io.Copy(ioutil.Discard, ch2.Reader)
		} else {
			io.Copy(os.Stdout, ch2.Reader)
		}
		ch2.Writer.Close()
	}

	wg.Wait()

	/*
		if counter != 2 {
			t.Errorf("Failed to grep keywords.")
		}
	*/
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
	ch1 := NewPiper()
	ch2 := NewPiper()

	wg.Add(1)
	go func() {
		fmt.Println("starting sending to ch1 ...")
		for _, d := range data {
			//WriteRow(ch1.Writer, d)
			ch1.Writer.Write(d)
			ch1.Writer.Write([]byte("\n"))
		}
		ch1.Writer.Close()
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
	go Execute(&wg, "testing luajit", cmd, ch1, ch2, true, false, os.Stderr)

	wg.Add(1)
	go func() {
		fmt.Println("start reading ch2...")
		ProcessMessage(ch2.Reader, func(d []byte) error {
			row, _ := DecodeRow(d)
			fmt.Println("ch2:", string(row[0].([]byte)))
			return nil
		})
		wg.Done()
	}()

	wg.Wait()
}
