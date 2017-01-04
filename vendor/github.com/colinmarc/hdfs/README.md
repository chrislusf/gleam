HDFS for Go
===========

[![GoDoc](https://godoc.org/github.com/colinmarc/hdfs/web?status.svg)](https://godoc.org/github.com/colinmarc/hdfs) [![build](https://travis-ci.org/colinmarc/hdfs.svg?branch=master)](https://travis-ci.org/colinmarc/hdfs)

This is a native golang client for hdfs. It connects directly to the namenode using
the protocol buffers API.

It tries to be idiomatic by aping the stdlib `os` package, where possible, and
implements the interfaces from it, including `os.FileInfo` and `os.PathError`.

Here's what it looks like in action:

```go
client, _ := hdfs.New("namenode:8020")

file, _ := client.Open("/mobydick.txt")

buf := make([]byte, 59)
file.ReadAt(buf, 48847)

fmt.Println(string(buf))
// => Abominable are the tumblers into which he pours his poison.
```

For complete documentation, check out the [Godoc][1].

The `hdfs` Binary
-----------------

Along with the library, this repo contains a commandline client for HDFS. Like
the library, its primary aim is to be idiomatic, by enabling your favorite unix
verbs:


    $ hdfs --help
    Usage: hdfs COMMAND
    The flags available are a subset of the POSIX ones, but should behave similarly.

    Valid commands:
      ls [-lah] [FILE]...
      rm [-rf] FILE...
      mv [-fT] SOURCE... DEST
      mkdir [-p] FILE...
      touch [-amc] FILE...
      chmod [-R] OCTAL-MODE FILE...
      chown [-R] OWNER[:GROUP] FILE...
      cat SOURCE...
      head [-n LINES | -c BYTES] SOURCE...
      tail [-n LINES | -c BYTES] SOURCE...
      du [-sh] FILE...
      checksum FILE...
      get SOURCE [DEST]
      getmerge SOURCE DEST
      put SOURCE DEST

Since it doesn't have to wait for the JVM to start up, it's also a lot faster
`hadoop -fs`:

    $ time hadoop fs -ls / > /dev/null

    real  0m2.218s
    user  0m2.500s
    sys 0m0.376s

    $ time hdfs ls / > /dev/null

    real  0m0.015s
    user  0m0.004s
    sys 0m0.004s

Best of all, it comes with bash tab completion for paths!

Installing
----------

To install the library, once you have Go [all set up][2]:

    $ go get github.com/colinmarc/hdfs

Or, to install just the commandline client:

    $ go get github.com/colinmarc/hdfs/cmd/hdfs


You'll also want to add two lines to your `.bashrc` or `.profile`:

    source $GOPATH/src/github.com/colinmarc/hdfs/cmd/hdfs/bash_completion
    export HADOOP_NAMENODE="namenode:8020"

Or, to install tab completion globally on linux:

    ln -sT $GOPATH/src/github.com/colinmarc/hdfs/cmd/hdfs/bash_completion /etc/bash_completion.d/gohdfs

By default, the HDFS user is set to the currently-logged-in user. You can
override this in your `.bashrc` or `.profile`:

    export HADOOP_USER_NAME=username

Compatibility
-------------

This library uses "Version 9" of the HDFS protocol, which means it should work
with hadoop distributions based on 2.2.x and above. The tests run against CDH
5.x and HDP 2.x.

Acknowledgements
----------------

This library is heavily indebted to [snakebite][3].

[1]: https://godoc.org/github.com/colinmarc/hdfs
[2]: https://golang.org/doc/install
[3]: https://github.com/spotify/snakebite
