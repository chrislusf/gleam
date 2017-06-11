package main

import (
	"os"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/cassandra"
)

func main() {

	gio.Init()

	c := cassandra.New("localhost", "system",
		"key, host_id, release_version, rpc_address, schema_version, tokens",
		"local", "", 8)

	f := flow.New().Read(c).Fprintf(os.Stdout,
		"key:%s\nhost_id:%v\nrelease_version:%s, %s, \nschema_version:%+v,\n%s \n")

	f.Run(distributed.Option())

}
