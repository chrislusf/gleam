package gio

import (
	"log"
	"os"
)

var (
	m map[string]func([]interface{}) error
)

func init() {
	m = make(map[string]func([]interface{}) error)
}

// RegisterMapper register a mapper function to process a command
func RegisterMapper(commandName string, fn func([]interface{}) error) {
	m[commandName] = fn
}

// Serve starts processing stdin and writes output to stdout
func Serve() {
	if len(os.Args) != 2 {
		log.Fatalf("Expecting one command lien arguments, but got %v", os.Args)
	}

	commandName := os.Args[1]

	fn, ok := m[commandName]
	if !ok {
		log.Fatalf("Failed to find function for %v", commandName)
	}

	if err := Process(fn); err != nil {
		log.Fatalf("Failed to execute %v: %v", os.Args, err)
	}

}
