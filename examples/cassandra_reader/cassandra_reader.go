package main

import (
	"flag"
	"os"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/cassandra"
)

var (
	hosts        = flag.String("hosts", "127.0.0.1", "a list of comma separated host names")
	keyspace     = flag.String("keyspace", "system", "the keyspace containing the table")
	table        = flag.String("from", "local", "the table name")
	selectClause = flag.String("select", "key, host_id, release_version, rpc_address, schema_version, tokens", "a list of field names")
	where        = flag.String("where", "", "optional where clause")
	limit        = flag.Int("limit", 10, "the number of rows to return. 0 means unlimited.")
	format       = flag.String("format", "key:%s\nhost_id:%v\nrelease_version:%s, %s, \nschema_version:%+v,\n%s \n", "a list of field names")
	timeout      = flag.Int("timeout", 30, "the number of seconds for timeout connections")
	concurrency  = flag.Int("concurrent", 2, "the number of concurrent read processes")
	shardCount   = flag.Int("shardCount", 4, "the number of shards to partition the data into")
)

func main() {

	gio.Init()
	flag.Parse()

	c := cassandra.Hosts(*hosts).Keyspace(*keyspace).Select(*selectClause).From(*table).Where(*where)
	c.ShardCount = *shardCount
	c.Concurrency = *concurrency
	c.LimitInEachShard = *limit
	c.TimeoutSeconds = *timeout

	f := flow.New().Read(c).Fprintlnf(os.Stdout, *format)

	f.Run()

}
