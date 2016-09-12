# gleam
another go based distributed execution system. working in progress.

# Why
Previously I created https://github.com/chrislusf/glow, a Go based distributed computing system. One thing I like is that 
it's all pure Go and statically compiled. However, the related problem that it is a fixed computation flow. 
There are no easy way to dynamically send a different computations to remote executors.

Gleam can resolve this issue. The computation can be done via Luajit, or common shell script(awk, sort, uniq,...),
or any custom program you want. Gleam provides an easy way to combine them together and distribute the work load.

Why Luajit? Luajit is easy to dispatch to remote executors. It has fairly good performance comparable to Java.
It's easy to learn also, trust me.

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
			if line then
				return line:gmatch("%w+")
			end
		end
	`).Pipe("sort").Pipe("uniq -c").SaveTextTo(os.Stdout, "%s")
}

```

# Understand the data format
The stdin and stdout are used to pass input and output. The data are passed around in "rows". Each row is a tuple of
(size, data), where data is []byte and size is the data's size. Each row's data is encoded in MsgPack format.

# FAQ
1. How to see the debug message?
Write to stderr, and do not write to stdout!

