# gleam
another go based distributed execution system. working in progress.

# Why
Previously I created https://github.com/chrislusf/glow, a Go based distributed computing system. One thing I like is that 
it's all pure Go and statically compiled. However, the related problem is that it is a fixed computation flow. 
There are no easy way to dynamically send a different computation to remote executors.

Gleam can resolve this issue. 

# Computation Execution
The computation can be done via Luajit, or common shell script(awk, sort, uniq,...), or any custom programs. Gleam provides an easy way to combine them together and distribute the work load.

## Luajit Execution
Luajit has the first class support. It has fairly good performance comparable to Java.
It's easy to learn also, trust me.

## Pipe Execution
This is basically the same as Unix's pipeline. You can use all basic unix tools, or create your own in Python/Ruby/Shell/Java/C/C++ ...

Nothing to talk about, really. You just need to output your results to stdout as lines.

For multiple values in a row, separate them via tab.

## Other Execution
Maybe not needed. Mostly you can just use Pipe(). However, current code allows customizable executions.
It's possible to support Python/Ruby/... same as Luajit.

# Status
Current version is only for single machine. Distributed version will come soon if this proves to be a good framework.

# Installation
1. Install Luajit
2. Put this customized MessagePack.lua under a folder where luajit can find it.
```
  https://github.com/chrislusf/gleam/blob/master/examples/tests/MessagePack.lua
  sudo cp MessagePack.lua /usr/local/share/luajit-2.0.4/
```
# Example

The full source code, not snippet, for word count:
```
package main

import (
	"os"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	flow.New().TextFile("/etc/passwd").FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Map(`
		function(word)
			return word, 1
		end
	`).Reduce(`
		function(x, y)
			return x + y
		end
	`).SaveTextTo(os.Stdout, "%s,%d")
}

```

Another way to do the similar:
```
package main

import (
	"os"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	flow.New().TextFile("/etc/passwd").FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Pipe("sort").Pipe("uniq -c").SaveTextTo(os.Stdout, "%s")
}

```


# Parallel Execution
One limitation for unix pipes is that they are easy for one single pipe, but not easy to parallel.

With Gleam this becomes very easy. (And this will be in distributed mode soon.)

This example get a list of file names, partitioned into 3 groups, and then process them in parallel.
This flow can be changed to use Pipe() also, of course.

```
// word_count.go
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/chrislusf/gleam/flow"
)

func main() {

	fileNames, err := filepath.Glob("/Users/chris/Downloads/txt/en/ep-08-*.txt")
	if err != nil {
		log.Fatal(err)
	}

	flow.New().Lines(fileNames).Partition(3).PipeAsArgs("cat $1").FlatMap(`
      function(line)
        return line:gmatch("%w+")
      end
    `).Map(`
      function(word)
        return word, 1
      end
    `).Reduce(`
      function(x, y)
        return x + y
      end
    `).SaveTextTo(os.Stdout, "%s\t%d")

}

```

# Currently Supported APIs
1. Pipe, PipeAsArgs
2. Map, ForEach, FlatMap
3. Filter
4. Reduce, LocalReduce
5. Partition
6. Sort, LocalSort, MergeSortedTo
7. Join, CoGroup


# FAQ
1. How to see the debug message?

# Understand the data format
Write to stderr, and do not write to stdout!

The stdin and stdout are used to pass input and output. The data are passed around in "rows". Each row is a tuple of
(size, data), where data is []byte and size is the data's size. Each row's data is encoded in MsgPack format.

