package main

import (
	"flag"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/cassandra"
)

var (
	hosts        = flag.String("hosts", "127.0.0.1", "a list of comma separated host names")
	keyspace     = flag.String("keyspace", "system_schema", "the keyspace containing the table")
	table        = flag.String("from", "tables", "the table name")
	selectClause = flag.String("select", "keyspace_name, table_name", "a list of field names")
	where        = flag.String("where", "", "optional where clause")
	limit        = flag.Int("limit", 10, "the number of rows to return. 0 means unlimited.")
	format       = flag.String("format", "keyspace:%v table: %v", "formatted output result")
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

	f := flow.New().Read(c).Printlnf(*format)

	f.Run()

}
