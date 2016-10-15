package csv

import (
	"os"
	"testing"

	"github.com/chrislusf/gleam"
)

func TestReadWithHeader(t *testing.T) {
	f := gleam.New()

	data := Read(f, []string{"sample1.csv"}, 1, true)
	data.Select(2, 3, 1).Fprintf(os.Stderr, "%s,%s,%s\n").Run()
}

func TestReadWithoutHeader(t *testing.T) {
	f := gleam.New()

	data := Read(f, []string{"sample0.csv"}, 1, false)
	data.Select(2, 3, 1, 1).Fprintf(os.Stderr, "%s,%s,%s,%s\n").Run()
}

func TestReadHeaderByFieldNames(t *testing.T) {
	f := gleam.New()

	data := Read(f, []string{"sample2.csv"}, 1, true,
		"statecode", "line", "construction", "policyID")
	data.Fprintf(os.Stderr, "%s,%s,%s,%s\n").Run()
}
