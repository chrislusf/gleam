package main

import (
	"os"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/plugins/cassandra"
)

func main() {
	// this is basically the same as defined in gleam.yaml
	// adapter.RegisterConnection("connection1", "cassandra").
	//	Set("keyspace", "system").Set("hosts", "localhost")

	f := flow.New().Query("connection1", &cassandra.Query{
		Select:   "key, host_id, release_version, rpc_address, schema_version, tokens",
		Keyspace: "system",
		Table:    "local",
		Parallel: 8,
	}).Fprintf(os.Stdout, "key:%s\nhost_id:%v\nrelease_version:%s, %s, \nschema_version:%+v,\n%s \n")

	// f.Run(distributed.Planner())

	f.Run(distributed.Option())

}
