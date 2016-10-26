# gleam
Gleam is a high performance and efficient distributed execution system, and also 
simple, generic, flexible and easy to customize.

Gleam is built in Go, and the user defined computation can be written in Lua, 
Unix pipe tools, or any streaming programs.

### High Performance

* Go itself has high performance and concurrency.
* LuaJit also has high performance comparable to C, Java, Go.

### Efficient

* Gleam does not have the common GC problem that plagued other languages. Each executor is run in a separated OS process.
* Gleam master and agent servers does not run actual computation.
* Gleam master and agent servers are memory efficient, consuming less than 10 MB memory.
* LuaJit runs in parallel OS processes. The memory is managed by the OS.
* One machine can host much more executors.

### Flexible
* The Gleam flow can run standalone or distributed.
* Data flows in Gleam either through memory and network, or optionally to disk for later re-tries.

### Easy to Customize
* The Go code is much simpler to read than Scala, Java, C++.
* LuaJIT FFI library can easily invoke any C functions, for even more performance or use any existing C libraries.
* (plan) Write SQL with UDF written in Lua.

# Architecture


# Documentation
* [Gleam Wiki] (https://github.com/chrislusf/gleam/wiki)
* [Installation](https://github.com/chrislusf/gleam/wiki/Installation)
* [Gleam Flow API GoDoc](https://godoc.org/github.com/chrislusf/gleam/flow)

# Standalone Example

## Word Count
The full source code, not snippet, for word count:
```
package main

import (
	"os"

	"github.com/chrislusf/gleam"
)

func main() {

	gleam.New().TextFile("/etc/passwd").FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Map(`
		function(word)
			return word, 1
		end
	`).ReduceBy(`
		function(x, y)
			return x + y
		end
	`).Fprintf(os.Stdout, "%s,%d\n").Run()
}

```

Another way to do the similar:
```
package main

import (
	"os"

	"github.com/chrislusf/gleam"
)

func main() {

	gleam.New().TextFile("/etc/passwd").FlatMap(`
		function(line)
			return line:gmatch("%w+")
		end
	`).Pipe("sort").Pipe("uniq -c").Fprintf(os.Stdout, "%s\n").Run()
}

```

## Join two CSV files. 

Assume there are file "a.csv" has fields "a1, a2, a3, a4, a5" and file "b.csv" has fields "b1, b2, b3". We want to join the rows where a1 = b2. And the output format should be "a1, a4, b3".

```
package main

import (
	"os"

	"github.com/chrislusf/gleam"
	"github.com/chrislusf/gleam/source/csv"
)

func main() {

	f := gleam.New()
	a := f.Input(csv.New("a.csv")).Select(1,4) // a1, a4
	b := f.Input(csv.New("b.csv")).Select(2,3) // b2, b3
	
	a.Join(b).Fprintf(os.Stdout, "%s,%s,%s\n").Run()  // a1, a4, b3

}

```

## Parallel Execution
One limitation for unix pipes is that they are easy for one single pipe, but not easy to parallel.

With Gleam this becomes very easy. (And this can be in distributed mode too!)

This example get a list of file names, partitioned into 3 groups, and then process them in parallel.
This flow can be changed to use Pipe() also, of course.

```
// word_count.go
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/chrislusf/gleam"
)

func main() {

	fileNames, err := filepath.Glob("/Users/chris/Downloads/txt/en/ep-08-*.txt")
	if err != nil {
		log.Fatal(err)
	}

	gleam.New().Lines(fileNames).Partition(3).PipeAsArgs("cat $1").FlatMap(`
      function(line)
        return line:gmatch("%w+")
      end
    `).Map(`
      function(word)
        return word, 1
      end
    `).ReduceBy(`
      function(x, y)
        return x + y
      end
    `).Fprintf(os.Stdout, "%s\t%d").Run()

}

```

# Distributed Computing
## Setup Gleam Cluster
Start a gleam master and serveral gleam agents
```
// start "gleam master" on a server
> go get github.com/chrislusf/gleam/distributed/gleam
> gleam master --address=":45326"

// start up "gleam agent" on some diffent server or port
// if a different server, remember to install Luajit and copy the MessagePack.lua file also.
> gleam agent --dir=2 --port 45327 --host=127.0.0.1
> gleam agent --dir=3 --port 45328 --host=127.0.0.1
```

## Change Execution Mode.
From gleam.New(), change to gleam.NewDistributed(), or gleam.New(gleam.Distributed)
```
  // local mode
  gleam.New()
  gleam.New(gleam.Local)
  
  // distributed mode
  gleam.NewDistributed()
  gleam.New(gleam.Distributed)
```
gleam.New(gleam.Local) and gleam.New(gleam.Distributed) are provided to dynamically change the execution mode.

# Status
Gleam is just beginning. Here are a few todo that needs help:
* Add better streaming support
* Add fault tolerant support
* Add better integration with Torch
* Add better SQL database support
* Add Luarock pacakging
* Add Python support
* Add Javascript support

Especially Need Help Now:
* Lua or Go implementation to read Parquet files

Help is needed. Anything is welcome. Small things count: fix documentation, adding a logo, adding docker image, blog about it, share it, etc.

[![](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=EEECLJ8QGTTPC) 

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
