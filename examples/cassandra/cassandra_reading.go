package main

import (
	"os"

	"github.com/chrislusf/gleam/adapter"
	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/plugins/cassandra"
)

func main() {
	adapter.RegisterConnection("c1", "cassandra").
		Set("keyspace", "system").Set("hosts", "localhost")

	f := flow.New()
	f.Query("c1", cassandra.Query{
		Select:   "key, host_id, release_version, rpc_address, schema_version, tokens",
		Keyspace: "system",
		Table:    "local",
		Parallel: 1,
	}).Fprintf(os.Stdout, "key:%s\nhost_id:%v\nrelease_version:%s, %s, \nschema_version:%+v,\n%s \n").
		Run(distributed.Option())

}
