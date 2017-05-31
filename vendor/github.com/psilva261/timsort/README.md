# timsort

**timsort** is a Go implementation of Tim Peters's mergesort
sorting algorithm.

For many input types it is 2-3 times faster than Go's built-in sorting.

The main drawback of this sort method is that it is not in-place (as any
mergesort), and may put extra strain on garbage collector.

This implementation was derived from Java's TimSort object by Josh Bloch,
which, in turn, was based on the [original code by Tim Peters][listsort].

## Installation

	$ go get github.com/psilva261/timsort

## Testing

Inside the source directory, type

	go test

to run test harness.

## Benchmarking

Inside the source directory, type

	go test -test.bench=.*

to run benchmarks. Each combination of input type/size is presented to timsort,
and, for comparison, to the standard Go sort (sort.Sort).
See [BENCHMARKS.md][BENCHMARKS.md] for more info and some benchmarking results.

## Example

	package main

	import (
		"github.com/psilva261/timsort"
		"fmt"
	)

	type Record struct {
		ssn  int
		name string
	}

	func BySsn(a, b interface{}) bool {
		return a.(Record).ssn < b.(Record).ssn
	}

	func ByName(a, b interface{}) bool {
		return a.(Record).name < b.(Record).name
	}

	func main() {
		db := make([]interface{}, 3)
		db[0] = Record{123456789, "joe"}
		db[1] = Record{101765430, "sue"}
		db[2] = Record{345623452, "mary"}

		// sorts array by ssn (ascending)
		timsort.Sort(db, BySsn)
		fmt.Printf("sorted by ssn: %v\n", db)

		// now re-sort same array by name (ascending)
		timsort.Sort(db, ByName)
		fmt.Printf("sorted by name: %v\n", db)
	}

[listsort]: http://svn.python.org/projects/python/trunk/Objects/listsort.txt
[BENCHMARKS.md]: http://github.com/psilva261/timsort/blob/master/BENCHMARKS.md
