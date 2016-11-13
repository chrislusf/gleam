package cassandra

import (
	"encoding/gob"
	"strings"

	"github.com/chrislusf/gleam/adapter"
	"github.com/gocql/gocql"
)

func init() {
	gob.Register(CassandraDataSplit{})

	adapter.RegisterAdapter("cassandra", func() adapter.Adapter {
		return NewCassandraAdapter()
	})
}

type Query struct {
	Select    string
	Keyspace  string
	Table     string
	Where     string
	Partition int
	Parallel  int
}

type CassandraDataSplit struct {
	Config                         map[string]string
	StartToken, StopToken          string
	Select, Keyspace, Table, Where string
	PartitionKeys                  []string
}

func (q *Query) GetParallelLimit() int {
	return q.Parallel
}

type CassandraAdapter struct {
	cluster *gocql.ClusterConfig
}

func NewCassandraAdapter() *CassandraAdapter {
	return &CassandraAdapter{}
}

func (c *CassandraAdapter) AdapterName() string {
	return "cassandra"
}

func (c *CassandraAdapter) LoadConfiguration(config map[string]string) {
	hosts := strings.Split(config["hosts"], ",")
	c.cluster = gocql.NewCluster(hosts...)
	c.cluster.Keyspace = config["keyspace"]
	c.cluster.ProtoVersion = 4
}

func (cs CassandraDataSplit) GetConfiguration() map[string]string {
	return cs.Config
}
